//! Worker-local preparation identities. This API does not establish cache eligibility,
//! readiness, resource cost, or a task duration. It never starts or joins work.
use crate::ids::{AttemptId, BuilderId, Digest, RunId, SourceId};
use crate::setup::observation::PreparationKind;
use crate::toolchain::contract::FrozenBundle;
use crate::wire::SetupSpec;
use serde::Serialize;
use std::io::{self, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

/// A complete supported-schema task context, not an attempt or accepted consumer.
/// No Debug implementation: setup and frozen inputs may contain private data.
pub(crate) struct PreparationProbeInput<'a> {
    pub manifest_digest: &'a Digest,
    pub source_id: &'a SourceId,
    pub run_id: &'a RunId,
    pub setup: &'a Option<SetupSpec>,
    pub frozen: &'a Option<Box<FrozenBundle>>,
    pub in_place: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeUnknown {
    MissingContext,
    InvalidBinding,
    ActivationUnverified,
    ProfileUnavailable,
    UnsafePath,
    IoUnavailable,
    BudgetExceeded,
    OwnershipChanged,
    Barrier,
}
#[allow(
    dead_code,
    reason = "Consumed by the next worker protocol slice; remove when its entrypoint is connected."
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparationProbe {
    /// No managed preparation; ordinary task steps may still perform setup.
    NoManagedPreparation,
    Unknown(ProbeUnknown),
    Entries {
        owner_generation: BuilderId,
        entries: Vec<PreparationProbeEntry>,
    },
}
#[allow(
    dead_code,
    reason = "Consumed by the next worker protocol slice; remove when its entrypoint is connected."
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparationProbeEntry {
    pub kind: PreparationKind,
    /// Hash of the actual internal key, exactly as in preparation observations.
    pub key: Option<Digest>,
    pub state: PreparationProbeState,
}
#[allow(
    dead_code,
    reason = "Consumed by the next worker protocol slice; remove when its entrypoint is connected."
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparationProbeState {
    /// Concrete identity only, never evidence that work is cold, warm or usable.
    ConcretePlan,
    Active {
        builder_id: BuilderId,
        builder_attempt_id: AttemptId,
    },
    Unknown(ProbeUnknown),
}

/// Lexical limits precede all IO. Component-by-component no-follow metadata is
/// bounded (32 operations), and rejects symlink ancestors rather than walking them.
/// Missing paths are allowed here; plan resolution still checks required roots.
pub(crate) fn bounded_path(path: &Path) -> Result<(), ProbeUnknown> {
    if !path.is_absolute() || path.as_os_str().len() > 4096 || path.components().count() > 32 {
        return Err(ProbeUnknown::BudgetExceeded);
    }
    let mut at = PathBuf::new();
    for part in path.components() {
        if !matches!(part, Component::RootDir | Component::Normal(_)) {
            return Err(ProbeUnknown::UnsafePath);
        }
        at.push(part);
        match std::fs::symlink_metadata(&at) {
            Ok(m) if m.file_type().is_symlink() => return Err(ProbeUnknown::UnsafePath),
            Ok(m) if m.mode() & 0o444 == 0 => return Err(ProbeUnknown::IoUnavailable),
            Ok(_) => (),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(ProbeUnknown::IoUnavailable),
        }
    }
    Ok(())
}

pub(crate) fn input_budget(input: &PreparationProbeInput<'_>) -> Result<(), ProbeUnknown> {
    // Streaming bound avoids an unbounded intermediate JSON allocation.
    struct Budget(usize);
    impl Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|n| *n <= 256 * 1024)
                .ok_or_else(|| io::Error::other("probe budget"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    fn bounded(value: &impl Serialize) -> Result<(), ProbeUnknown> {
        serde_json::to_writer(Budget(0), value).map_err(|_| ProbeUnknown::BudgetExceeded)
    }
    if input
        .setup
        .as_ref()
        .is_some_and(|s| s.steps.len() > 64 || s.toolchain.len() > 32 || s.secrets.len() > 32)
        || input.frozen.as_ref().is_some_and(|b| {
            b.evidence.len() > 16
                || b.frozen.requirements.len() > 16
                || b.frozen.selections.len() > 16
        })
    {
        return Err(ProbeUnknown::BudgetExceeded);
    }
    // Output cardinality is separate from the serialized-input and shared FD
    // byte budgets. The optional managed workspace entry consumes one slot too.
    let workspace = !input.in_place
        && input
            .setup
            .as_ref()
            .is_some_and(|s| !super::policy::hoisted(s).is_empty());
    let tools = input.frozen.as_ref().map_or(0, |b| b.evidence.len());
    if tools + usize::from(workspace) > 16 {
        return Err(ProbeUnknown::BudgetExceeded);
    }
    bounded(&(
        input.manifest_digest,
        input.source_id,
        input.run_id,
        input.setup,
        input.frozen,
    ))
}

pub(crate) fn environment_budget(
    spec: &SetupSpec,
    env: &std::collections::BTreeMap<String, String>,
) -> Result<(), ProbeUnknown> {
    if env.len() > 256
        || env
            .iter()
            .map(|(k, v)| k.len().saturating_add(v.len()))
            .sum::<usize>()
            > 65536
    {
        return Err(ProbeUnknown::BudgetExceeded);
    }
    let dirs: Vec<_> = env
        .get("PATH")
        .map(String::as_str)
        .unwrap_or("")
        .split(':')
        .collect();
    if dirs.len() > 64 {
        return Err(ProbeUnknown::BudgetExceeded);
    }
    // At most 32 declared tools x 64 directories x 32 path components, twice.
    for dir in dirs.into_iter().filter(|d| d.starts_with('/')) {
        bounded_path(Path::new(dir))?;
        for tool in &spec.toolchain {
            if tool.name.is_empty() || tool.name.len() > 128 || tool.name.contains('/') {
                return Err(ProbeUnknown::InvalidBinding);
            }
            bounded_path(&Path::new(dir).join(&tool.name))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};
    #[test]
    fn preparation_probe_paths_are_bounded_nofollow_and_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap();
        assert_eq!(bounded_path(&root.join("absent")), Ok(()));
        let link = root.join("link");
        symlink(&root, &link).unwrap();
        assert_eq!(
            bounded_path(&link.join("child")),
            Err(ProbeUnknown::UnsafePath)
        );
        assert_eq!(
            bounded_path(Path::new("relative")),
            Err(ProbeUnknown::BudgetExceeded)
        );
        assert_eq!(
            bounded_path(&root.join((0..33).map(|_| "d").collect::<Vec<_>>().join("/"))),
            Err(ProbeUnknown::BudgetExceeded)
        );
        let file = root.join("private");
        std::fs::write(&file, b"x").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o0)).unwrap();
        assert_eq!(bounded_path(&file), Err(ProbeUnknown::IoUnavailable));
    }
}
