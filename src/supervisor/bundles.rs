//! Bounded fd-relative generic output selection and TarV1 production.
//! TarV1 contains sorted regular files, GNU headers with mode 0600 and zero
//! uid/gid/mtime, plus library-produced GNU long-name records where necessary.
//! No directory, link, sparse, PAX or global metadata entries are produced.
use super::outcome::{store_artifact, ArtifactInput, Forbidden, OutcomeError, Result};
use super::spec::ArtifactsSpec;
use crate::ids::AttemptId;
use crate::outputs::*;
use crate::secrets::place::FileIdentity;
use crate::source::safety::fold_path;
use crate::wire::{ArtifactEncoding, ArtifactRecord, OutputDecl};
use rustix::fs::{open, openat, Dir, FileType, Mode, OFlags};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};

const DIR: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
const LEAF: OFlags = OFlags::RDONLY
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK)
    .union(OFlags::CLOEXEC);
fn refused() -> OutcomeError {
    OutcomeError::BadSource(PathBuf::from("output bundle"))
}

struct Selection<'a> {
    patterns: globset::GlobSet,
    literals: &'a [String],
    parents: Vec<String>,
    roots: Vec<String>,
    secrets: &'a [String],
    selected: BTreeMap<String, std::fs::Metadata>,
    folded: BTreeMap<Vec<u8>, String>,
    visited: usize,
}
impl Selection<'_> {
    fn matches(&self, path: &str) -> bool {
        self.patterns.is_match(path)
            || self.literals.iter().any(|p| {
                path.strip_prefix(p.as_str())
                    .is_some_and(|s| s.starts_with('/'))
            })
    }
    fn descend(&self, path: &str) -> bool {
        self.roots.iter().any(|root| {
            root.is_empty()
                || root == path
                || root.starts_with(&format!("{path}/"))
                || path.starts_with(&format!("{root}/"))
        })
    }
    fn walk(&mut self, directory: &File, prefix: &str) -> Result<()> {
        for entry in Dir::read_from(directory).map_err(std::io::Error::from)? {
            let entry = entry.map_err(std::io::Error::from)?;
            let bytes = entry.file_name().to_bytes();
            if matches!(bytes, b"." | b"..") {
                continue;
            }
            self.visited += 1;
            if self.visited > MAX_VISITED {
                return Err(refused());
            }
            let name = std::str::from_utf8(bytes).map_err(|_| refused())?;
            let path = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            };
            let selected = self.matches(&path);
            // Runner metadata is never traversed or archived, even via an explicit declaration.
            if crate::outputs::RESERVED_COMPONENTS.contains(&fold_path(name.as_bytes()).as_slice())
            {
                if selected || self.parents.contains(&path) {
                    return Err(refused());
                } else {
                    continue;
                }
            }
            let kind = entry.file_type();
            if kind == FileType::Directory && !self.descend(&path) {
                continue;
            }
            if !valid_path(&path) {
                return Err(refused());
            }
            if kind == FileType::Directory {
                let child: File = openat(directory, entry.file_name(), DIR, Mode::empty())
                    .map_err(std::io::Error::from)?
                    .into();
                self.walk(&child, &path)?;
            } else if selected
                || self.parents.contains(&path)
                || self
                    .literals
                    .iter()
                    .any(|p| p.starts_with(&format!("{path}/")))
            {
                if !selected || kind != FileType::RegularFile {
                    return Err(refused());
                }
                if self
                    .secrets
                    .iter()
                    .any(|s| fold_path(s.as_bytes()) == fold_path(path.as_bytes()))
                {
                    return Err(refused());
                }
                let file: File = openat(directory, entry.file_name(), LEAF, Mode::empty())
                    .map_err(std::io::Error::from)?
                    .into();
                if !file.metadata()?.is_file() {
                    return Err(refused());
                }
                // Include implicit parent directory spellings in the collision set.
                let mut prefix = String::new();
                for component in path.split('/') {
                    if !prefix.is_empty() {
                        prefix.push('/');
                    }
                    prefix.push_str(component);
                    if self
                        .folded
                        .insert(fold_path(prefix.as_bytes()), prefix.clone())
                        .is_some_and(|old| old != prefix)
                    {
                        return Err(refused());
                    }
                }
                self.selected.insert(path, file.metadata()?);
                if self.selected.len() > MAX_FILES {
                    return Err(refused());
                }
            }
        }
        Ok(())
    }
}

pub fn store_bundle(
    root: &Path,
    attempt: &AttemptId,
    plan: &ArtifactsSpec,
    declaration: &OutputDecl,
    forbidden: &Forbidden,
    remaining: &mut usize,
) -> Result<ArtifactRecord> {
    let patterns = patterns(&declaration.paths).map_err(|_| refused())?;
    let literals: Vec<String> = declaration
        .paths
        .iter()
        .filter(|p| !p.contains(['*', '?', '[', '{']))
        .cloned()
        .collect();
    let workspace: File = open(&plan.workspace, DIR, Mode::empty())
        .map_err(std::io::Error::from)?
        .into();
    let mut selection = Selection {
        patterns,
        roots: declaration
            .paths
            .iter()
            .map(|p| {
                p.split('/')
                    .take_while(|c| !c.contains(['*', '?', '[', '{']))
                    .collect::<Vec<_>>()
                    .join("/")
            })
            .collect(),
        parents: declaration
            .paths
            .iter()
            .flat_map(|p| {
                let mut prefix = String::new();
                let mut parts = Vec::new();
                for c in p.split('/') {
                    if c.contains(['*', '?', '[', '{']) {
                        break;
                    }
                    if !prefix.is_empty() {
                        prefix.push('/');
                    }
                    prefix.push_str(c);
                    parts.push(prefix.clone());
                }
                parts
            })
            .collect(),
        literals: &literals,
        secrets: &plan.secret_paths,
        selected: BTreeMap::new(),
        folded: BTreeMap::new(),
        visited: 0,
    };
    if plan.secret_paths.iter().any(|s| selection.matches(s)) {
        return Err(refused());
    }
    selection.walk(&workspace, "")?;
    if selection.selected.is_empty() {
        return Err(OutcomeError::SourceMissing(PathBuf::from("output bundle")));
    }
    if selection.selected.len() > *remaining {
        return Err(refused());
    }
    let selected_count = selection.selected.len();
    let names: Vec<&str> = selection.selected.keys().map(String::as_str).collect();
    super::spool_plan::check_artifact_names(&names).map_err(|_| refused())?;
    // Named bundle temp lives in the attempt's private artifact directory;
    // the existing crash sweep owns these .tmp files. Member staging is unnamed.
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(root)?;
    super::outcome::ensure_real_dirs(
        root,
        &[
            std::ffi::OsStr::new(attempt.as_str()),
            std::ffi::OsStr::new("artifacts"),
        ],
    )?;
    let private = super::outcome::artifacts_root(root, attempt);
    let bundle = tempfile::Builder::new()
        .prefix(".tmp-bundle-")
        .tempfile_in(&private)?;
    let mut archive = tar::Builder::new(bundle.as_file().try_clone()?);
    let max = plan.max_bytes.min(MAX_BUNDLE_BYTES);
    for (path, selected_meta) in selection.selected {
        if forbidden.values.contains(path.as_bytes()) {
            return Err(refused());
        }
        let mut source = crate::platform::open_beneath_dir(&workspace, Path::new(&path))?;
        let before = source.metadata()?;
        if FileIdentity::of(&before) != FileIdentity::of(&selected_meta) {
            return Err(refused());
        }
        if !before.is_file()
            || before.nlink() != 1
            || forbidden.files.contains(&FileIdentity::of(&before))
        {
            return Err(refused());
        }
        if before.len() > max {
            return Err(refused());
        }
        let mut staged = tempfile::tempfile_in(&private)?;
        let mut finder = forbidden.values.finder();
        let mut bytes = 0u64;
        let mut buf = [0u8; 64 << 10];
        loop {
            let n = source.read(&mut buf)?;
            if n == 0 {
                break;
            }
            bytes += n as u64;
            if bytes > max || finder.push(&buf[..n]) {
                return Err(refused());
            }
            staged.write_all(&buf[..n])?;
        }
        let after = source.metadata()?;
        if bytes != before.len()
            || before.len() != after.len()
            || before.mtime() != after.mtime()
            || before.mtime_nsec() != after.mtime_nsec()
            || before.ctime() != after.ctime()
            || before.ctime_nsec() != after.ctime_nsec()
            || after.nlink() != 1
        {
            return Err(refused());
        }
        staged.seek(SeekFrom::Start(0))?;
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o600);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(bytes);
        header.set_cksum();
        // Account for the next member, optional GNU longname and final zero blocks
        // before writing, so even a refused bundle never exceeds its fixed budget.
        let name_meta = if path.len() > 100 {
            512 + ((path.len() as u64 + 1).div_ceil(512) * 512)
        } else {
            0
        };
        let next = archive
            .get_mut()
            .stream_position()?
            .checked_add(512 + bytes.div_ceil(512) * 512 + name_meta + 1024)
            .ok_or_else(refused)?;
        if next > max {
            return Err(refused());
        }
        archive.append_data(&mut header, &path, staged)?;
    }
    archive.finish()?;
    archive.into_inner()?.sync_all()?;
    let safe = Forbidden {
        files: Vec::new(),
        values: forbidden.values.clone(),
    };
    let input = ArtifactInput {
        name: &declaration.name,
        source: bundle.path(),
        source_id: plan.source_id.clone(),
        plan_digest: plan.plan_digest.clone(),
        profile_fingerprint: plan.profile_fingerprint.clone(),
        forbidden: &safe,
    };
    let mut record = store_artifact(root, attempt, &private, &input, max)?;
    record.encoding = ArtifactEncoding::TarV1;
    *remaining -= selected_count;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{Digest, SourceId};
    use crate::secrets::mask::Masker;
    use crate::wire::{MissingPolicy, OutputWhen};
    use std::fs;
    use std::os::unix::fs::symlink;
    fn plan(workspace: &Path) -> ArtifactsSpec {
        ArtifactsSpec {
            workspace: workspace.into(),
            source_id: SourceId::generate(),
            plan_digest: Digest::of_bytes(b"plan"),
            profile_fingerprint: Digest::of_bytes(b"profile"),
            max_bytes: MAX_BUNDLE_BYTES,
            files: vec![],
            outputs: vec![],
            secret_files: vec![],
            secret_paths: vec![],
        }
    }
    fn decl(paths: &[&str]) -> OutputDecl {
        OutputDecl {
            name: "reports".into(),
            paths: paths.iter().map(|p| (*p).into()).collect(),
            if_missing: MissingPolicy::Required,
            when: OutputWhen::Always,
        }
    }
    fn store(
        root: &Path,
        plan: &ArtifactsSpec,
        paths: &[&str],
        forbidden: &Forbidden,
    ) -> Result<ArtifactRecord> {
        let mut remaining = MAX_ATTEMPT_FILES;
        store_bundle(
            root,
            &AttemptId::generate(),
            plan,
            &decl(paths),
            forbidden,
            &mut remaining,
        )
    }
    #[test]
    fn union_dedup_sorted_long_names_and_deterministic_headers() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path().join("ws");
        let root = dir.path().join("spool");
        fs::create_dir_all(ws.join("reports/deep")).unwrap();
        fs::create_dir_all(ws.join("coverage")).unwrap();
        fs::write(ws.join("reports/a"), b"alpha").unwrap();
        fs::write(ws.join("coverage/b"), b"beta").unwrap();
        let long = format!("reports/deep/{}", "l".repeat(150));
        fs::write(ws.join(&long), b"long").unwrap();
        let p = plan(&ws);
        let a = store(
            &root,
            &p,
            &["reports/**", "coverage/**", "reports"],
            &Forbidden::nothing(),
        )
        .unwrap();
        let b = store(
            &root,
            &p,
            &["reports", "coverage/**", "reports/**"],
            &Forbidden::nothing(),
        )
        .unwrap();
        assert_eq!(a.encoding, ArtifactEncoding::TarV1);
        assert_eq!(a.digest, b.digest);
        let archive = root.join(a.attempt_id.as_str()).join("artifacts/reports");
        let mut archive = tar::Archive::new(File::open(archive).unwrap());
        let mut paths = vec![];
        for e in archive.entries().unwrap() {
            let mut e = e.unwrap();
            assert!(e.header().entry_type().is_file());
            assert_eq!(e.header().mode().unwrap(), 0o600);
            assert_eq!(e.header().uid().unwrap(), 0);
            assert_eq!(e.header().gid().unwrap(), 0);
            assert_eq!(e.header().mtime().unwrap(), 0);
            paths.push(e.path().unwrap().to_str().unwrap().to_owned());
            let mut data = Vec::new();
            e.read_to_end(&mut data).unwrap();
            assert!(!data.is_empty());
        }
        assert_eq!(
            paths,
            vec!["coverage/b".to_owned(), "reports/a".to_owned(), long]
        );
    }
    #[test]
    fn rejects_secret_paths_inodes_links_and_cross_buffer_values() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path().join("ws");
        let root = dir.path().join("spool");
        fs::create_dir_all(&ws).unwrap();
        fs::write(ws.join("secret"), b"TOPSECRET").unwrap();
        let mut p = plan(&ws);
        p.secret_paths = vec!["secret".into()];
        assert!(store(&root, &p, &["secret"], &Forbidden::nothing()).is_err());
        let forbidden = Forbidden {
            files: vec![FileIdentity::of(&fs::metadata(ws.join("secret")).unwrap())],
            values: Masker::new([b"TOPSECRET".to_vec()]),
        };
        fs::rename(ws.join("secret"), ws.join("alias")).unwrap();
        assert!(store(&root, &p, &["alias"], &forbidden).is_err());
        fs::write(ws.join("ordinary"), b"fine").unwrap();
        fs::hard_link(ws.join("ordinary"), ws.join("hard")).unwrap();
        assert!(store(&root, &p, &["hard"], &forbidden).is_err());
        let mut content = vec![b'x'; (64 << 10) - 3];
        content.extend_from_slice(b"TOPSECRET");
        fs::write(ws.join("copy"), content).unwrap();
        assert!(store(&root, &p, &["copy"], &forbidden).is_err());
        fs::write(ws.join("TOPSECRET-report"), b"safe").unwrap();
        assert!(store(&root, &p, &["TOPSECRET-report"], &forbidden).is_err());
        // Every refused bundle's own named temp is removed.
        for entry in fs::read_dir(&root).unwrap() {
            let path = entry.unwrap().path().join("artifacts");
            if path.exists() {
                assert_eq!(fs::read_dir(path).unwrap().count(), 0);
            }
        }
    }
    #[test]
    fn refuses_selected_hostile_entries_and_bounds_but_does_not_follow_git() {
        use std::os::unix::ffi::OsStrExt;
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path().join("ws");
        let root = dir.path().join("spool");
        fs::create_dir_all(ws.join(".git")).unwrap();
        fs::write(ws.join(".git/token"), b"private").unwrap();
        fs::write(ws.join("good"), b"fine").unwrap();
        let mut p = plan(&ws);
        assert!(store(&root, &p, &[".git"], &Forbidden::nothing()).is_err());
        assert!(matches!(
            store(&root, &p, &[".git/**", "good"], &Forbidden::nothing()),
            Err(OutcomeError::BadSource(_))
        ));
        symlink("/etc/passwd", ws.join("link")).unwrap();
        assert!(store(&root, &p, &["link"], &Forbidden::nothing()).is_err());
        assert!(matches!(
            store(&root, &p, &["link/child"], &Forbidden::nothing()),
            Err(OutcomeError::BadSource(_))
        ));
        let socket = std::os::unix::net::UnixListener::bind(ws.join("socket")).unwrap();
        assert!(store(&root, &p, &["socket"], &Forbidden::nothing()).is_err());
        drop(socket);
        let fifo = std::ffi::CString::new(ws.join("fifo").as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert!(store(&root, &p, &["fifo"], &Forbidden::nothing()).is_err());
        assert!(matches!(
            store(&root, &p, &["absent"], &Forbidden::nothing()),
            Err(OutcomeError::SourceMissing(_))
        ));
        assert!(store(&root, &p, &["good"], &Forbidden::nothing()).is_ok());
        p.max_bytes = 1024;
        assert!(store(&root, &p, &["good"], &Forbidden::nothing()).is_err());
        p.max_bytes = MAX_BUNDLE_BYTES;
        let mut remaining = 0;
        assert!(store_bundle(
            &root,
            &AttemptId::generate(),
            &p,
            &decl(&["good"]),
            &Forbidden::nothing(),
            &mut remaining
        )
        .is_err());
        match fs::write(ws.join(std::ffi::OsStr::from_bytes(b"bad\xff")), b"x") {
            Ok(()) => assert!(store(&root, &p, &["good"], &Forbidden::nothing()).is_err()),
            Err(e) => assert_eq!(e.raw_os_error(), Some(libc::EILSEQ)), // macOS refuses non-UTF8 names at creation.
        }
    }
    #[test]
    fn traversal_file_count_depth_and_member_byte_limits_refuse_without_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path().join("ws");
        fs::create_dir(&ws).unwrap();
        fs::write(ws.join("good"), b"fine").unwrap();
        let root = dir.path().join("spool");
        let p = plan(&ws);
        let mut selection = Selection {
            patterns: patterns(&["good".into()]).unwrap(),
            literals: &[],
            parents: vec![],
            roots: vec!["good".into()],
            secrets: &[],
            selected: BTreeMap::new(),
            folded: BTreeMap::new(),
            visited: MAX_VISITED,
        };
        assert!(selection.walk(&File::open(&ws).unwrap(), "").is_err());
        selection.visited = 0;
        let metadata = fs::metadata(ws.join("good")).unwrap();
        selection.selected = (0..MAX_FILES)
            .map(|n| (format!("seed{n}"), metadata.clone()))
            .collect();
        assert!(selection.walk(&File::open(&ws).unwrap(), "").is_err());
        let huge = File::create(ws.join("huge")).unwrap();
        huge.set_len(MAX_BUNDLE_BYTES + 1).unwrap();
        assert!(store(&root, &p, &["huge"], &Forbidden::nothing()).is_err());
        let deep = ws.join(vec!["deep"; MAX_DEPTH].join("/"));
        fs::create_dir_all(&deep).unwrap();
        fs::write(deep.join("file"), b"x").unwrap();
        assert!(store(&root, &p, &["deep"], &Forbidden::nothing()).is_err());
        // An unrelated deep input tree must not stop literal-prefix output selection.
        assert!(store(&root, &p, &["good"], &Forbidden::nothing()).is_ok());
        let mut remaining = 1;
        let blocked = Forbidden {
            files: vec![],
            values: Masker::new([b"fine".to_vec()]),
        };
        assert!(store_bundle(
            &root,
            &AttemptId::generate(),
            &p,
            &decl(&["good"]),
            &blocked,
            &mut remaining
        )
        .is_err());
        assert_eq!(remaining, 1);
    }
}
