//! Pure generic output contract shared by admission, execution and collection.
use crate::state::Outcome;
use crate::wire::{ArtifactDecl, OutputDecl, OutputWhen};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

pub const MAX_OUTPUTS: usize = 1024;
pub const MAX_PATH_BYTES: usize = 1024;
pub const MAX_DEPTH: usize = 16;
pub const MAX_FILES: usize = 4096;
pub const MAX_ATTEMPT_FILES: usize = 16384;
pub const MAX_VISITED: usize = 100_000;
pub const MAX_BUNDLE_BYTES: u64 = 1 << 30;
/// Components excluded from producer traversal and retained bundle export.
pub const RESERVED_COMPONENTS: [&[u8]; 2] = [b".git", b".remote-task-runner"];
pub const FEATURE: &str = "output_bundles_v1";

pub fn eligible(when: OutputWhen, outcome: Outcome, collection_allowed: bool) -> bool {
    if !collection_allowed {
        return false;
    }
    match when {
        OutputWhen::Always => true,
        OutputWhen::Success => outcome == Outcome::Succeeded,
        OutputWhen::Failure => matches!(outcome, Outcome::Failed | Outcome::TimedOut),
    }
}

pub fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= MAX_PATH_BYTES
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
        && path.split('/').all(|c| !matches!(c, "" | "." | ".."))
        && path.split('/').count() <= MAX_DEPTH
}

pub fn patterns(paths: &[String]) -> Result<GlobSet, &'static str> {
    if paths.is_empty() || paths.len() > MAX_OUTPUTS {
        return Err("invalid output pattern count");
    }
    let mut builder = GlobSetBuilder::new();
    for p in paths {
        if !valid_path(p) {
            return Err("output path is not a bounded relative path");
        }
        builder.add(
            GlobBuilder::new(p)
                .literal_separator(true)
                .backslash_escape(false)
                .build()
                .map_err(|_| "invalid output glob")?,
        );
    }
    builder.build().map_err(|_| "invalid output glob set")
}

pub fn validate(raw: &[ArtifactDecl], outputs: &[OutputDecl]) -> Result<(), &'static str> {
    crate::worker::policy::check_artifacts(raw)?;
    if raw.len() + outputs.len() > MAX_OUTPUTS {
        return Err("too many logical outputs");
    }
    let mut count = 0usize;
    let mut names: Vec<&str> = raw.iter().map(|d| d.name.as_str()).collect();
    for d in outputs {
        if d.name.len() > 255
            || !valid_path(&d.name)
            || crate::supervisor::spool_plan::relative_path(&d.name).is_none()
        {
            return Err("invalid output name");
        }
        count += d.paths.len();
        if count > MAX_OUTPUTS {
            return Err("too many output patterns");
        }
        patterns(&d.paths)?;
        names.push(&d.name);
    }
    crate::supervisor::spool_plan::check_artifact_names(&names)
        .map_err(|_| "conflicting output names")
}

/// The frozen output policies participate in a new command plan's identity.
/// An empty output list retains the original command-only representation.
pub fn command_plan_digest(
    command: &[String],
    outputs: &[OutputDecl],
) -> Result<crate::ids::Digest, serde_json::Error> {
    let bytes = if outputs.is_empty() {
        serde_json::to_vec(command)?
    } else {
        serde_json::to_vec(&(command, outputs))?
    };
    Ok(crate::ids::Digest::of_bytes(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::MissingPolicy;
    #[test]
    fn conditions() {
        for outcome in [
            Outcome::Succeeded,
            Outcome::Failed,
            Outcome::TimedOut,
            Outcome::Cancelled,
            Outcome::Interrupted,
        ] {
            for when in [OutputWhen::Always, OutputWhen::Success, OutputWhen::Failure] {
                assert!(!eligible(when, outcome, false));
            }
            assert!(eligible(OutputWhen::Always, outcome, true));
            assert_eq!(
                eligible(OutputWhen::Success, outcome, true),
                outcome == Outcome::Succeeded
            );
            assert_eq!(
                eligible(OutputWhen::Failure, outcome, true),
                matches!(outcome, Outcome::Failed | Outcome::TimedOut)
            );
        }
    }
    #[test]
    fn paths_and_globs() {
        for p in ["", "/a", "a/", "a//b", "a/../b", ".", "a\\b", "a\nb"] {
            assert!(!valid_path(p), "{p}");
        }
        assert!(!valid_path(&"a".repeat(1025)));
        assert!(!valid_path(&vec!["a"; 17].join("/")));
        assert!(valid_path(".hidden/a"));
        assert!(valid_path(&"a".repeat(1024)));
        assert!(valid_path(&vec!["a"; 16].join("/")));
        assert!(patterns(&[]).is_err());
        assert!(patterns(&vec!["a".into(); 1025]).is_err());
        assert!(patterns(&["[".into()]).is_err());
        assert!(patterns(&["../a".into()]).is_err());
        let p = patterns(&["a/*".into(), "b/**".into()]).unwrap();
        assert!(p.is_match("a/x"));
        assert!(!p.is_match("a/x/y"));
        assert!(p.is_match("b/x/y"));
    }
    #[test]
    fn fixed_bounds_match_the_wire_contract() {
        assert_eq!(MAX_BUNDLE_BYTES, 1_073_741_824);
        assert_eq!(
            (
                MAX_OUTPUTS,
                MAX_PATH_BYTES,
                MAX_DEPTH,
                MAX_FILES,
                MAX_ATTEMPT_FILES,
                MAX_VISITED
            ),
            (1024, 1024, 16, 4096, 16384, 100_000)
        );
        assert_eq!(FEATURE, "output_bundles_v1");
    }
    #[test]
    fn declarations() {
        let d = OutputDecl {
            name: "reports".into(),
            paths: vec!["a".into()],
            if_missing: MissingPolicy::Required,
            when: OutputWhen::Always,
        };
        assert!(validate(&[], &[]).is_ok());
        assert!(validate(&[], std::slice::from_ref(&d)).is_ok());
        assert!(validate(&[], &vec![d.clone(); 1025]).is_err());
        assert!(validate(&[], &[d.clone(), d.clone()]).is_err());
        let mut bad = d.clone();
        bad.name = "".into();
        assert!(validate(&[], &[bad]).is_err());
        let mut exact = d.clone();
        exact.name = "a".repeat(255);
        assert!(validate(&[], std::slice::from_ref(&exact)).is_ok());
        let mut bad = d.clone();
        bad.name = format!("{}/{}", "a".repeat(128), "b".repeat(128));
        assert!(validate(&[], &[bad]).is_err());
        let mut bad = d.clone();
        bad.name = "a".repeat(256);
        assert!(validate(&[], &[bad]).is_err());
        let mut bad = d.clone();
        bad.name = ".tmp-name".into();
        assert!(validate(&[], &[bad]).is_err());
        let mut bad = d.clone();
        bad.paths.clear();
        assert!(validate(&[], &[bad]).is_err());
        let raw = ArtifactDecl {
            name: d.name.clone(),
            path: "a".into(),
            if_missing: MissingPolicy::Optional,
        };
        assert!(validate(std::slice::from_ref(&raw), std::slice::from_ref(&d)).is_err());
        let mut badraw = raw;
        badraw.path = "../a".into();
        assert!(validate(&[badraw], &[]).is_err());
        let mut many = d.clone();
        many.paths = vec!["a".into(); 1024];
        assert!(validate(&[], std::slice::from_ref(&many)).is_ok());
        let count: Vec<_> = (0..1024)
            .map(|i| {
                let mut x = d.clone();
                x.name = format!("n{i}");
                x
            })
            .collect();
        assert!(validate(&[], &count).is_ok());
        let raw_many: Vec<_> = (0..512)
            .map(|i| ArtifactDecl {
                name: format!("raw{i}"),
                path: format!("path{i}"),
                if_missing: MissingPolicy::Required,
            })
            .collect();
        assert!(validate(&raw_many, &count[..512]).is_ok());
        assert!(validate(&raw_many, &count[..513]).is_err());
        let mut many = d.clone();
        many.paths = vec!["a".into(); 1024];
        let mut other = d;
        other.name = "other".into();
        assert!(validate(&[], &[many, other]).is_err());
    }
}

#[cfg(test)]
mod digest_tests {
    use super::*;
    use crate::wire::MissingPolicy;
    #[test]
    fn output_fields_and_policies_bind_the_plan_and_payload() {
        let command = vec!["true".to_owned()];
        let d = OutputDecl {
            name: "out".into(),
            paths: vec!["reports/**".into()],
            if_missing: MissingPolicy::Required,
            when: OutputWhen::Always,
        };
        assert_eq!(
            command_plan_digest(&command, &[]).unwrap(),
            crate::ids::Digest::of_bytes(&serde_json::to_vec(&command).unwrap())
        );
        let digest = command_plan_digest(&command, std::slice::from_ref(&d)).unwrap();
        assert_ne!(digest, command_plan_digest(&command, &[]).unwrap());
        for field in 0..4 {
            let mut changed = d.clone();
            match field {
                0 => changed.name = "other".into(),
                1 => changed.paths.push("coverage/**".into()),
                2 => changed.if_missing = MissingPolicy::Optional,
                _ => changed.when = OutputWhen::Failure,
            };
            assert_ne!(digest, command_plan_digest(&command, &[changed]).unwrap());
        }
    }
}
