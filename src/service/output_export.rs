//! Safe opaque-output export. Full bundle preflight precedes any destination write.
use crate::error::{ApiError, ErrorCode};
use crate::storage::query::Artifact;
use crate::wire::ArtifactEncoding;
use rustix::fs::{mkdirat, open, openat, unlinkat, AtFlags, Mode, OFlags};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use unicode_normalization::UnicodeNormalization;

fn internal(e: impl ToString) -> ApiError {
    ApiError::new(ErrorCode::Internal, e.to_string())
}
fn invalid(e: impl ToString) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, e.to_string())
}
fn conflict(e: impl ToString) -> ApiError {
    ApiError::new(ErrorCode::Conflict, e.to_string())
}
const DIR: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

/// An unnamed private spool is held throughout validation and publication.
pub struct Bundle {
    file: File,
    members: Vec<Member>,
}
struct Member {
    path: String,
    offset: u64,
    size: u64,
}
impl Bundle {
    pub fn verify(source: File, artifact: &Artifact) -> Result<Self, ApiError> {
        Self::verify_cancellable(source, artifact, &AtomicBool::new(false))
    }
    pub fn verify_cancellable(
        mut source: File,
        artifact: &Artifact,
        cancelled: &AtomicBool,
    ) -> Result<Self, ApiError> {
        if artifact.size > crate::outputs::MAX_BUNDLE_BYTES {
            return Err(internal("output exceeds bundle limit"));
        }
        let mut file = tempfile::tempfile().map_err(internal)?;
        let mut hash = Sha256::new();
        let mut count = 0u64;
        let mut buffer = [0; 65536];
        loop {
            if cancelled.load(Ordering::Relaxed) {
                return Err(internal("export detached"));
            }
            let n = source.read(&mut buffer).map_err(internal)?;
            if n == 0 {
                break;
            }
            count += n as u64;
            if count > artifact.size {
                return Err(internal("output size mismatch"));
            }
            hash.update(&buffer[..n]);
            file.write_all(&buffer[..n]).map_err(internal)?;
        }
        let digest = crate::supervisor::spool_plan::sha256_digest(&hash.finalize())
            .ok_or_else(|| internal("digest conversion"))?;
        if count != artifact.size || digest != artifact.digest {
            return Err(internal("output digest/size mismatch"));
        }
        file.seek(SeekFrom::Start(0)).map_err(internal)?;
        let members = match artifact.encoding {
            ArtifactEncoding::Raw => vec![Member {
                path: String::new(),
                offset: 0,
                size: artifact.size,
            }],
            ArtifactEncoding::TarV1 => Self::preflight(&mut file, count, cancelled)?,
        };
        Ok(Self { file, members })
    }
    fn preflight(
        file: &mut File,
        size: u64,
        cancelled: &AtomicBool,
    ) -> Result<Vec<Member>, ApiError> {
        let mut members = Vec::new();
        let mut seen = BTreeMap::<String, (String, bool)>::new();
        let mut long = None;
        let mut end = 0;
        {
            let mut archive = tar::Archive::new(&mut *file);
            for entry in archive.entries().map_err(internal)?.raw(true) {
                if cancelled.load(Ordering::Relaxed) {
                    return Err(internal("export detached"));
                }
                let mut entry = entry.map_err(internal)?;
                let length = entry.size();
                let offset = entry.raw_file_position();
                let kind = entry.header().entry_type();
                let gnu = entry
                    .header()
                    .as_gnu()
                    .ok_or_else(|| internal("tar_v1 requires GNU headers"))?;
                if gnu.is_extended()
                    || gnu.realsize.iter().any(|b| *b != 0)
                    || gnu
                        .sparse
                        .iter()
                        .any(|s| s.offset.iter().chain(&s.numbytes).any(|b| *b != 0))
                    || gnu.linkname.iter().any(|b| *b != 0)
                {
                    return Err(internal("unsupported GNU sparse/link metadata"));
                }
                if kind.is_gnu_longname() {
                    if long.is_some()
                        || length == 0
                        || length > (crate::outputs::MAX_PATH_BYTES + 1) as u64
                        || entry.path_bytes().as_ref() != b"././@LongLink"
                    {
                        return Err(internal("invalid GNU long name"));
                    }
                    let mut bytes = Vec::with_capacity(length as usize);
                    entry.read_to_end(&mut bytes).map_err(internal)?;
                    if bytes.len() as u64 != length || bytes.pop() != Some(0) || bytes.contains(&0)
                    {
                        return Err(internal("invalid GNU long name bytes"));
                    }
                    long = Some(String::from_utf8(bytes).map_err(internal)?);
                } else {
                    if !kind.is_file()
                        || kind.as_byte() != b'0'
                        || length > crate::outputs::MAX_BUNDLE_BYTES
                        || members.len() == crate::outputs::MAX_FILES
                    {
                        return Err(internal("unsupported tar member or limit exceeded"));
                    }
                    let path = match long.take() {
                        Some(path) => path,
                        None => std::str::from_utf8(&entry.path_bytes())
                            .map_err(internal)?
                            .to_owned(),
                    };
                    if !crate::outputs::valid_path(&path)
                        || path.split('/').any(|c| {
                            c.len() > 255
                                || crate::outputs::RESERVED_COMPONENTS.contains(
                                    &crate::source::safety::fold_path(c.as_bytes()).as_slice(),
                                )
                        })
                    {
                        return Err(internal("unsafe tar member path"));
                    }
                    let components: Vec<_> = path.split('/').collect();
                    for i in 1..=components.len() {
                        let spelling = components[..i].join("/");
                        let folded = spelling
                            .nfc()
                            .flat_map(char::to_lowercase)
                            .collect::<String>();
                        let leaf = i == components.len();
                        if let Some((old, old_leaf)) = seen.get(&folded) {
                            if old != &spelling || *old_leaf || leaf {
                                return Err(internal("duplicate/case/prefix tar collision"));
                            }
                        } else {
                            seen.insert(folded, (spelling, leaf));
                        }
                    }
                    let copied =
                        std::io::copy(&mut entry, &mut std::io::sink()).map_err(internal)?;
                    if copied != length {
                        return Err(internal("short tar member"));
                    }
                    members.push(Member {
                        path,
                        offset,
                        size: length,
                    });
                }
                end = offset
                    .checked_add(length.div_ceil(512) * 512)
                    .ok_or_else(|| internal("tar offset overflow"))?;
                if end > size {
                    return Err(internal("truncated tar member"));
                }
            }
        }
        if members.is_empty() || long.is_some() || size < end + 1024 || !size.is_multiple_of(512) {
            return Err(internal("missing tar terminator"));
        }
        file.seek(SeekFrom::Start(end)).map_err(internal)?;
        let mut buffer = [0; 65536];
        loop {
            if cancelled.load(Ordering::Relaxed) {
                return Err(internal("export detached"));
            }
            let n = file.read(&mut buffer).map_err(internal)?;
            if n == 0 {
                break;
            }
            if buffer[..n].iter().any(|b| *b != 0) {
                return Err(internal("trailing tar garbage"));
            }
        }
        Ok(members)
    }
    /// Continue after a concurrent destination conflict; preserve all successes.
    pub fn publish(
        &mut self,
        root: &File,
        prefix: &str,
        cancelled: &AtomicBool,
        mut outcome: impl FnMut(&str, Result<(), ApiError>) -> bool,
    ) {
        for member in &self.members {
            if cancelled.load(Ordering::Relaxed) {
                break;
            }
            let path = if member.path.is_empty() {
                prefix.to_owned()
            } else {
                format!("{prefix}/{}", member.path)
            };
            let result = (|| {
                let (parent, leaf) = path
                    .rsplit_once('/')
                    .ok_or_else(|| invalid("missing parent"))?;
                let directory = walk(root, parent)?;
                let temp = format!(".rtr-export-{}", crate::ids::AttemptId::generate());
                let mut output: File = openat(
                    &directory,
                    &temp,
                    OFlags::WRONLY
                        | OFlags::CREATE
                        | OFlags::EXCL
                        | OFlags::NOFOLLOW
                        | OFlags::CLOEXEC,
                    Mode::from_raw_mode(0o600),
                )
                .map_err(internal)?
                .into();
                let result = (|| {
                    self.file
                        .seek(SeekFrom::Start(member.offset))
                        .map_err(internal)?;
                    let mut copied = 0;
                    let mut input = (&mut self.file).take(member.size);
                    let mut buffer = [0; 65536];
                    loop {
                        if cancelled.load(Ordering::Relaxed) {
                            return Err(internal("export detached"));
                        }
                        let n = input.read(&mut buffer).map_err(internal)?;
                        if n == 0 {
                            break;
                        }
                        output.write_all(&buffer[..n]).map_err(internal)?;
                        copied += n as u64;
                    }
                    if copied != member.size {
                        return Err(internal("short private spool"));
                    }
                    output.sync_all().map_err(internal)?;
                    if cancelled.load(Ordering::Relaxed) {
                        return Err(internal("export detached"));
                    }
                    rustix::fs::linkat(&directory, &temp, &directory, leaf, AtFlags::empty())
                        .map_err(|e| {
                            if e == rustix::io::Errno::EXIST {
                                conflict("destination already exists")
                            } else {
                                internal(e)
                            }
                        })?;
                    directory.sync_all().map_err(internal)
                })();
                // Only this invocation's exclusive temporary name is removed.
                let cleanup = unlinkat(&directory, &temp, AtFlags::empty()).map_err(internal);
                result.and(cleanup)
            })();
            if !outcome(&path, result) {
                break;
            }
        }
    }
}
fn walk(base: &File, path: &str) -> Result<File, ApiError> {
    let mut directory: File = openat(base, ".", DIR, Mode::empty())
        .map_err(internal)?
        .into();
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(invalid("unsafe destination component"));
        }
        match mkdirat(&directory, component, Mode::from_raw_mode(0o700)) {
            Ok(()) => (),
            Err(e) if e == rustix::io::Errno::EXIST => (),
            Err(e) => return Err(internal(e)),
        }
        directory = openat(&directory, component, DIR, Mode::empty())
            .map_err(|e| conflict(format!("unsafe destination directory: {e}")))?
            .into();
    }
    Ok(directory)
}
/// Every root ancestor is traversed with NOFOLLOW, including the chosen root.
pub fn destination(path: &Path) -> Result<File, ApiError> {
    let base: File = open(
        if path.is_absolute() { "/" } else { "." },
        DIR,
        Mode::empty(),
    )
    .map_err(internal)?
    .into();
    let mut directory = base;
    for component in path.components() {
        match component {
            std::path::Component::RootDir | std::path::Component::CurDir => (),
            std::path::Component::Normal(name) => {
                let name = name
                    .to_str()
                    .ok_or_else(|| invalid("non UTF-8 destination"))?;
                directory = walk(&directory, name)?;
            }
            _ => return Err(invalid("destination parent traversal is unsupported")),
        }
    }
    Ok(directory)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::*;

    use std::os::unix::fs::{symlink, PermissionsExt};
    fn record(bytes: &[u8], encoding: ArtifactEncoding) -> Artifact {
        Artifact {
            encoding,
            name: "bundle".into(),
            size: bytes.len() as u64,
            digest: Digest::of_bytes(bytes),
        }
    }
    fn verify(bytes: &[u8], encoding: ArtifactEncoding) -> Result<Bundle, ApiError> {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(bytes).unwrap();
        file.rewind().unwrap();
        Bundle::verify(file, &record(bytes, encoding))
    }
    fn tar(entries: &[(&str, &[u8], u8)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, bytes, kind) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o600);
            header.set_entry_type(tar::EntryType::new(*kind));
            header.set_uid(0);
            header.set_gid(0);
            header.set_mtime(0);
            if path.len() <= 100 {
                header.as_gnu_mut().unwrap().name[..path.len()].copy_from_slice(path.as_bytes());
                header.set_cksum();
                builder.append(&header, *bytes).unwrap();
            } else {
                builder.append_data(&mut header, path, *bytes).unwrap();
            }
        }
        builder.into_inner().unwrap()
    }
    #[test]
    fn raw_never_sniffs_tar_and_no_replace_preserves_foreign_bytes() {
        let bytes = tar(&[("a", b"content", b'0')]);
        let mut bundle = verify(&bytes, ArtifactEncoding::Raw).unwrap();
        let dir = tempfile::tempdir_in("/private/tmp").unwrap();
        let root = destination(dir.path()).unwrap();
        let mut outcomes = vec![];
        bundle.publish(
            &root,
            "task/attempt/raw",
            &AtomicBool::new(false),
            |_, r| {
                outcomes.push(r);
                true
            },
        );
        assert!(outcomes[0].is_ok());
        assert_eq!(
            std::fs::read(dir.path().join("task/attempt/raw")).unwrap(),
            bytes
        );
        bundle.publish(
            &root,
            "task/attempt/raw",
            &AtomicBool::new(false),
            |_, r| {
                assert_eq!(r.unwrap_err().code, ErrorCode::Conflict);
                true
            },
        );
        assert_eq!(
            std::fs::read(dir.path().join("task/attempt/raw")).unwrap(),
            bytes
        );
        assert_eq!(
            std::fs::metadata(dir.path().join("task"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(dir.path().join("task/attempt/raw"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    #[test]
    fn tar_long_paths_dotfiles_and_partial_conflicts() {
        let long = format!("{}/file", "a".repeat(120));
        let bytes = tar(&[
            (".tmp-user", b"dot", b'0'),
            (&long, b"long", b'0'),
            ("z", b"last", b'0'),
        ]);
        let mut bundle = verify(&bytes, ArtifactEncoding::TarV1).unwrap();
        let dir = tempfile::tempdir_in("/private/tmp").unwrap();
        let root = destination(dir.path()).unwrap();
        std::fs::create_dir_all(dir.path().join("task/attempt/bundle")).unwrap();
        std::fs::write(dir.path().join("task/attempt/bundle/z"), b"foreign").unwrap();
        let mut success = 0;
        let mut conflict = 0;
        bundle.publish(
            &root,
            "task/attempt/bundle",
            &AtomicBool::new(false),
            |_, r| {
                match r {
                    Ok(()) => success += 1,
                    Err(e) => {
                        assert_eq!(e.code, ErrorCode::Conflict);
                        conflict += 1
                    }
                }
                true
            },
        );
        assert_eq!((success, conflict), (2, 1));
        assert_eq!(
            std::fs::read(dir.path().join("task/attempt/bundle/.tmp-user")).unwrap(),
            b"dot"
        );
        assert_eq!(
            std::fs::read(dir.path().join(format!("task/attempt/bundle/{long}"))).unwrap(),
            b"long"
        );
        assert_eq!(
            std::fs::read(dir.path().join("task/attempt/bundle/z")).unwrap(),
            b"foreign"
        );
    }
    #[test]
    fn rejects_path_type_collision_and_trailing_before_publication() {
        for entries in [
            vec![("../escape", b"x".as_slice(), b'0')],
            vec![("/absolute", b"x".as_slice(), b'0')],
            vec![("a", b"x".as_slice(), b'2')],
            vec![("a", b"x".as_slice(), b'1')],
            vec![("a", b"x".as_slice(), b'5')],
            vec![("a", b"x".as_slice(), b'x')],
            vec![("a", b"x".as_slice(), b'0'), ("a", b"y".as_slice(), b'0')],
            vec![("a", b"x".as_slice(), b'0'), ("A/b", b"y".as_slice(), b'0')],
            vec![
                ("A/x", b"x".as_slice(), b'0'),
                ("a/y", b"y".as_slice(), b'0'),
            ],
            vec![("a/b", b"x".as_slice(), b'0'), ("a", b"y".as_slice(), b'0')],
            vec![
                ("é", b"x".as_slice(), b'0'),
                ("e\u{301}", b"y".as_slice(), b'0'),
            ],
        ] {
            assert!(
                verify(&tar(&entries), ArtifactEncoding::TarV1)
                    .err()
                    .unwrap()
                    .code
                    == ErrorCode::Internal,
                "{entries:?}"
            );
        }
        let bytes = tar(&[("a", b"x", b'0')]);
        for len in [0, 511, 512, 1023, 1535] {
            assert_eq!(
                verify(&bytes[..len], ArtifactEncoding::TarV1)
                    .err()
                    .unwrap()
                    .code,
                ErrorCode::Internal
            );
        }
        let mut garbage = bytes.clone();
        *garbage.last_mut().unwrap() = 1;
        assert_eq!(
            verify(&garbage, ArtifactEncoding::TarV1)
                .err()
                .unwrap()
                .code,
            ErrorCode::Internal
        );
        let mut bad = bytes;
        bad[0] ^= 1;
        assert_eq!(
            verify(&bad, ArtifactEncoding::TarV1).err().unwrap().code,
            ErrorCode::Internal
        );
    }
    #[test]
    fn reserved_members_are_corruption_before_any_publication() {
        for path in [
            ".git/config",
            "a/.git/config",
            ".remote-task-runner/token",
            "a/.remote-task-runner/token",
            "a/.GiT/config",
            "a/.g\u{200d}it/config",
            "a/.remote-task-runner\u{feff}/token",
            "a/.REMOTE-TASK-RUNNER/token",
        ] {
            // A valid member preceding the malicious member must not become visible.
            let bytes = tar(&[("ordinary", b"ok", b'0'), (path, b"private", b'0')]);
            let dir = tempfile::tempdir_in("/private/tmp").unwrap();
            let root = destination(dir.path()).unwrap();
            let error = match verify(&bytes, ArtifactEncoding::TarV1) {
                Err(error) => error,
                Ok(mut bundle) => {
                    bundle.publish(&root, "bundle", &AtomicBool::new(false), |_, _| true);
                    panic!("reserved archive accepted: {path}");
                }
            };
            assert_eq!(error.code, ErrorCode::Internal, "{path}");
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        }
    }
    #[test]
    fn refuses_wrong_outer_digest_size_and_symlink_ancestors() {
        let bytes = b"binary\0\xff";
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(bytes).unwrap();
        file.rewind().unwrap();
        let mut meta = record(bytes, ArtifactEncoding::Raw);
        meta.digest = Digest::of_bytes(b"other");
        assert!(Bundle::verify(file, &meta).is_err());
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(bytes).unwrap();
        file.rewind().unwrap();
        meta.size += 1;
        assert!(Bundle::verify(file, &meta).is_err());
        let dir = tempfile::tempdir_in("/private/tmp").unwrap();
        let outside = tempfile::tempdir_in("/private/tmp").unwrap();
        symlink(outside.path(), dir.path().join("link")).unwrap();
        assert!(destination(&dir.path().join("link/root")).is_err());
        let root = destination(dir.path()).unwrap();
        let mut bundle = verify(bytes, ArtifactEncoding::Raw).unwrap();
        bundle.publish(&root, "link/out", &AtomicBool::new(false), |_, r| {
            assert_eq!(r.unwrap_err().code, ErrorCode::Conflict);
            true
        });
        assert!(!outside.path().join("out").exists());
    }
    #[test]
    fn archive_limits_and_cancelled_verification_fail_closed() {
        let paths = (0..=crate::outputs::MAX_FILES)
            .map(|n| format!("file{n:04}"))
            .collect::<Vec<_>>();
        let entries = paths
            .iter()
            .map(|path| (path.as_str(), b"".as_slice(), b'0'))
            .collect::<Vec<_>>();
        assert!(verify(&tar(&entries), ArtifactEncoding::TarV1).is_err());
        let depth = vec!["a"; 17].join("/");
        let long = "a".repeat(1025);
        for path in [depth, long] {
            assert!(verify(&tar(&[(&path, b"x", b'0')]), ArtifactEncoding::TarV1).is_err());
        }
        let bytes = b"x";
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(bytes).unwrap();
        file.rewind().unwrap();
        assert!(Bundle::verify_cancellable(
            file,
            &record(bytes, ArtifactEncoding::Raw),
            &AtomicBool::new(true)
        )
        .is_err());
        let mut meta = record(bytes, ArtifactEncoding::Raw);
        meta.size = crate::outputs::MAX_BUNDLE_BYTES + 1;
        assert!(Bundle::verify(tempfile::tempfile().unwrap(), &meta).is_err());
    }
    #[test]
    fn concurrent_publish_has_exactly_one_winner() {
        let dir = tempfile::tempdir_in("/private/tmp").unwrap();
        let root = std::sync::Arc::new(destination(dir.path()).unwrap());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let jobs = [b"one".as_slice(), b"two".as_slice()]
            .into_iter()
            .map(|bytes| {
                let root = root.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let mut bundle = verify(bytes, ArtifactEncoding::Raw).unwrap();
                    barrier.wait();
                    let mut result = None;
                    bundle.publish(&root, "task/out", &AtomicBool::new(false), |_, r| {
                        result = Some(r);
                        true
                    });
                    result.unwrap()
                })
            })
            .collect::<Vec<_>>();
        let results = jobs
            .into_iter()
            .map(|job| job.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|r| r.as_ref().is_err_and(|e| e.code == ErrorCode::Conflict))
                .count(),
            1
        );
        let bytes = std::fs::read(dir.path().join("task/out")).unwrap();
        assert!(bytes == b"one" || bytes == b"two");
        assert_eq!(
            std::fs::read_dir(dir.path().join("task")).unwrap().count(),
            1
        );
    }
    #[test]
    fn cancelled_export_never_creates_member() {
        let dir = tempfile::tempdir_in("/private/tmp").unwrap();
        let root = destination(dir.path()).unwrap();
        let mut bundle = verify(b"x", ArtifactEncoding::Raw).unwrap();
        bundle.publish(&root, "task/output", &AtomicBool::new(true), |_, _| {
            panic!("cancelled")
        });
        assert!(!dir.path().join("task").exists());
    }
}
