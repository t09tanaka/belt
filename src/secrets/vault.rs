//! The parent's hold on a run's secret files (spec 22.3, "親での保持").
//!
//! local-ci writes the bundle of a capture into the source store, keyed by
//! source: two runs with the same input would share one file, and the end of
//! the first would take the bundle from the second. So the run takes its own
//! copy (`<state>/secrets/<run>.json`, 0600) and the store's copy is removed at
//! once. A retry of the run reads the same copy, never the working tree again.
//! The copy is deleted when the run is terminal.

mod guard;
#[cfg(test)]
pub(crate) use guard::set_retention_fault;
#[cfg(test)]
pub(crate) use guard::set_sweep_fault;
pub(crate) use guard::{retention_fault, Claim, Lease};
use std::sync::Arc;

use super::envfile::{secret_values, Values};
use super::key::ClusterKey;
use super::plan::{check_limits, check_paths, ct_eq, hex_decode, listing_digest, to_wire_digest};
use super::{Result, SecretError};
use crate::ids::{Digest, RunId};
use crate::wire::{Bytes, HmacDigest, SecretFile};
use local_ci::source::{secret_digest, RelPath, SecretEntry};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// A temporary file older than this is a leftover of a crash.
pub(crate) const STALE_TMP_AGE: Duration = Duration::from_secs(600);

/// The files of one run with their keyed digests, ready to send. `Debug`
/// names the paths and never shows a content.
#[derive(Clone)]
pub struct HeldBundle {
    pub files: Vec<SecretFile>,
    retained: Option<crate::protocol::sched::Retained>,
}

impl PartialEq for HeldBundle {
    fn eq(&self, other: &Self) -> bool {
        self.files == other.files
    }
}
impl Eq for HeldBundle {}
impl std::fmt::Debug for HeldBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldBundle")
            .field(
                "paths",
                &self
                    .files
                    .iter()
                    .map(|x| x.path.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl HeldBundle {
    pub(crate) fn retained(&self) -> Option<crate::protocol::sched::Retained> {
        self.retained.clone()
    }
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Digest of the listing (paths and HMACs, not contents), for the storage
    /// reference of this bundle.
    pub fn listing_digest(&self) -> Digest {
        let entries: Vec<(String, HmacDigest)> = self
            .files
            .iter()
            .map(|f| (f.path.clone(), f.hmac_digest.clone()))
            .collect();
        listing_digest(&entries)
    }

    /// The strings to mask in logs: the `KEY=VALUE` values of every file, and
    /// the keys whose value was too short to mask.
    pub fn mask_values(&self) -> Values {
        let mut all = Values::default();
        let mut skipped = BTreeSet::new();
        for f in &self.files {
            let v = secret_values(&f.content.0);
            for x in v.values {
                // no second copy of a value: a short scan, not a set of clones
                if !all.values.contains(&x) {
                    all.values.push(x);
                }
            }
            skipped.extend(v.skipped_keys);
        }
        all.skipped_keys = skipped.into_iter().collect();
        all
    }
}

#[derive(Deserialize)]
struct Stored {
    schema_version: u32,
    files: Vec<StoredFile>,
}

#[derive(Deserialize)]
struct StoredFile {
    path: StoredPath,
    content_hex: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StoredPath {
    Text(String),
    Hex { hex: String },
}

impl StoredPath {
    fn rel(&self) -> Option<RelPath> {
        match self {
            StoredPath::Text(s) => Some(RelPath(s.clone().into_bytes())),
            StoredPath::Hex { hex } => hex_decode(hex).map(RelPath),
        }
    }
}

fn unreadable() -> SecretError {
    SecretError::Format("the secret bundle is not readable".into())
}

/// A regular file (not a link) that only its owner can access.
pub(crate) fn read_private(path: &Path, what: &'static str) -> Result<Vec<u8>> {
    let meta = fs::symlink_metadata(path).map_err(|e| {
        if e.kind() == ErrorKind::NotFound {
            SecretError::Unavailable {
                what: what.to_string(),
            }
        } else {
            SecretError::io(format!("{what} {}", path.display()), e)
        }
    })?;
    if !meta.file_type().is_file() || meta.mode() & 0o077 != 0 {
        return Err(SecretError::Insecure {
            what,
            path: path.to_path_buf(),
        });
    }
    fs::read(path).map_err(|e| SecretError::io(format!("{what} {}", path.display()), e))
}

/// Write `bytes` to `path` as 0600, all at once: a reader never sees half of
/// it, and an existing file is replaced.
pub(crate) fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| SecretError::Format("secret path has no directory".into()))?;
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy())
        .unwrap_or_default();
    let tmp = dir.join(format!(".{name}.{}.tmp", ulid::Ulid::new()));
    let io = |e| SecretError::io(format!("secret file {}", path.display()), e);
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(io)?;
    let r = f.write_all(bytes).and_then(|()| f.sync_all());
    drop(f);
    let r = r.and_then(|()| fs::rename(&tmp, path));
    if let Err(e) = r {
        let _ = fs::remove_file(&tmp);
        return Err(io(e));
    }
    // the rename itself must survive a crash too
    sync_dir(dir).map_err(io)
}

/// Make the entries of directory `dir` durable.
pub(crate) fn sync_dir(dir: &Path) -> std::io::Result<()> {
    fs::File::open(dir)?.sync_all()
}

/// Remove the temporary files (`.<name>.<ulid>.tmp`, regular files only) of
/// `dir` last written more than `max_age` ago: a crash between creating one
/// and renaming it leaves a plaintext copy behind. Returns how many.
pub(crate) fn sweep_stale_tmp(dir: &Path, max_age: Duration) -> Result<usize> {
    let io = |e| SecretError::io(format!("directory {}", dir.display()), e);
    let now = SystemTime::now();
    let mut removed = 0;
    for entry in fs::read_dir(dir).map_err(io)? {
        let entry = entry.map_err(io)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !(name.starts_with('.') && name.ends_with(".tmp")) {
            continue;
        }
        let Ok(meta) = fs::symlink_metadata(entry.path()) else {
            continue;
        };
        let age = meta
            .modified()
            .ok()
            .and_then(|m| now.duration_since(m).ok())
            .unwrap_or(Duration::ZERO);
        if meta.file_type().is_file() && age >= max_age {
            match fs::remove_file(entry.path()) {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => return Err(io(e)),
            }
        }
    }
    Ok(removed)
}

pub struct Vault {
    dir: PathBuf,
    root: Arc<std::fs::File>,
}

impl Vault {
    /// `<state_dir>/secrets`, made (0700) when missing; one that others can
    /// read, or that is a link, is refused.
    pub fn open(state_dir: &Path) -> Result<Vault> {
        let dir = state_dir.join("secrets");
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&dir)
            .map_err(|e| SecretError::io(format!("directory {}", dir.display()), e))?;
        let meta = fs::symlink_metadata(&dir)
            .map_err(|e| SecretError::io(format!("directory {}", dir.display()), e))?;
        if !meta.file_type().is_dir()
            || meta.mode() & 0o077 != 0
            || meta.uid() != rustix::process::geteuid().as_raw()
        {
            return Err(SecretError::Insecure {
                what: "secret directory",
                path: dir,
            });
        }
        let dir = std::fs::canonicalize(&dir)
            .map_err(|e| SecretError::io("resolve Vault directory", e))?;
        let root = Arc::new(
            rustix::fs::open(
                &dir,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::DIRECTORY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map(std::fs::File::from)
            .map_err(|e| SecretError::io("open Vault directory", e.into()))?,
        );
        let vault = Vault { dir, root };
        let lease = vault.lease(false)?;
        // Keep the cause: "unavailable" alone does not say which file to look at.
        lease
            .check_parent()
            .map_err(|error| SecretError::Unavailable {
                what: format!("registered parent Vault ({error:#})"),
            })?;
        drop(lease);
        Ok(vault)
    }

    #[cfg(test)]
    fn path(&self, run: &RunId) -> PathBuf {
        self.dir.join(format!("{run}.json"))
    }

    pub(crate) fn lease(&self, exclusive: bool) -> Result<Arc<Lease>> {
        Lease::acquire(&self.dir, Arc::clone(&self.root), exclusive)
            .map(Arc::new)
            .map_err(|error| {
                let busy = error.chain().any(|e| {
                    e.downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == ErrorKind::WouldBlock)
                });
                SecretError::io(
                    "Vault ownership",
                    if busy {
                        std::io::Error::from(ErrorKind::WouldBlock)
                    } else {
                        std::io::Error::other(error)
                    },
                )
            })
    }
    pub(crate) fn matches_source(&self, source: &Path) -> bool {
        self.dir.parent() == source.parent()
    }
    fn available(&self, lease: &Lease, run: &RunId) -> Result<()> {
        lease.available(run).map_err(|_| SecretError::Unavailable {
            what: format!("run {run}"),
        })
    }
    pub(crate) fn ensure_run_available(&self, run: &RunId) -> Result<()> {
        self.available(self.lease(false)?.as_ref(), run)
    }
    pub fn has(&self, run: &RunId) -> bool {
        self.lease(false).ok().is_some_and(|lease| {
            self.available(&lease, run).is_ok() && lease.has(run).unwrap_or(false)
        })
    }

    /// Take the bundle local-ci wrote at `store_copy` for `run`, and check it
    /// against the manifest's `entries` under `key` (see [`Vault::load`]): a
    /// copy that does not verify is not kept. A run that already holds a
    /// bundle keeps it (a retry must not change it). The store's plaintext
    /// copy is removed whatever the outcome, an error included.
    pub fn adopt(
        &self,
        run: &RunId,
        store_copy: &Path,
        entries: &[SecretEntry],
        key: &ClusterKey,
    ) -> Result<()> {
        let taken = (|| {
            let lease = self.lease(false)?;
            self.available(&lease, run)?;
            self.take(&lease, run, store_copy, entries, key)
        })();
        let removed = match fs::remove_file(store_copy) {
            Err(e) if e.kind() != ErrorKind::NotFound => Err(SecretError::io(
                format!("secret bundle {}", store_copy.display()),
                e,
            )),
            _ => Ok(()),
        };
        taken.and(removed)
    }

    fn take(
        &self,
        lease: &Arc<Lease>,
        run: &RunId,
        store_copy: &Path,
        entries: &[SecretEntry],
        key: &ClusterKey,
    ) -> Result<()> {
        let bytes = read_private(store_copy, "secret bundle")?;
        serde_json::from_slice::<Stored>(&bytes).map_err(|_| unreadable())?;
        if lease.has(run).map_err(|e| {
            e.downcast::<SecretError>()
                .unwrap_or_else(|e| SecretError::io("read Vault bundle", std::io::Error::other(e)))
        })? {
            return Ok(());
        }
        // Verify before publication. This also avoids trying to upgrade a
        // parent SH lease to EX for error cleanup while adoption still holds SH.
        Self::decode(&bytes, entries, key)?;
        lease
            .publish(&format!("{run}.json"), &bytes)
            .map_err(|e| SecretError::io("publish Vault bundle", std::io::Error::other(e)))?;
        Ok(())
    }

    /// The files of `run`, checked against the manifest's `entries`: the same
    /// paths, and every HMAC equal to the one recomputed under `key`. A run
    /// whose manifest names no secret file has an empty bundle. A bundle that
    /// is missing is `Unavailable` (`blocked: secret_unavailable`): never an
    /// empty or current-from-disk replacement.
    pub fn load(
        &self,
        run: &RunId,
        entries: &[SecretEntry],
        key: &ClusterKey,
    ) -> Result<HeldBundle> {
        self.load_owned(self.lease(false)?, run, entries, key)
    }
    fn load_owned(
        &self,
        lease: Arc<Lease>,
        run: &RunId,
        entries: &[SecretEntry],
        key: &ClusterKey,
    ) -> Result<HeldBundle> {
        self.available(&lease, run)?;
        let retained = Some(crate::protocol::sched::Retained::new(Arc::clone(&lease)));
        if entries.is_empty() {
            return Ok(HeldBundle {
                files: vec![],
                retained,
            });
        }
        let bytes = lease
            .bundle(run)
            .map_err(|e| {
                e.downcast::<SecretError>().unwrap_or_else(|e| {
                    SecretError::io("read Vault bundle", std::io::Error::other(e))
                })
            })?
            .ok_or_else(|| SecretError::Unavailable {
                what: format!("run {run}"),
            })?;
        Ok(HeldBundle {
            files: Self::decode(&bytes, entries, key)?,
            retained,
        })
    }
    fn decode(bytes: &[u8], entries: &[SecretEntry], key: &ClusterKey) -> Result<Vec<SecretFile>> {
        let stored: Stored = serde_json::from_slice(bytes).map_err(|_| unreadable())?;
        if stored.schema_version != 1 {
            return Err(SecretError::Format(format!(
                "secret bundle schema {} is not supported",
                stored.schema_version
            )));
        }
        let mut have: Vec<(RelPath, Vec<u8>)> = Vec::new();
        for f in stored.files {
            let rel = f.path.rel().ok_or_else(unreadable)?;
            let content = hex_decode(&f.content_hex).ok_or_else(unreadable)?;
            have.push((rel, content));
        }
        let want: BTreeSet<&RelPath> = entries.iter().map(|e| &e.path).collect();
        let got: BTreeSet<&RelPath> = have.iter().map(|(p, _)| p).collect();
        if want != got || have.len() != entries.len() {
            return Err(SecretError::PathSetMismatch);
        }
        let mut files = Vec::new();
        for (rel, content) in have {
            let entry = entries
                .iter()
                .find(|e| e.path == rel)
                .ok_or(SecretError::PathSetMismatch)?;
            let path = String::from_utf8(rel.0.clone())
                .map_err(|_| SecretError::NonUtf8Path(rel.display()))?;
            let digest = secret_digest(key.as_bytes(), &rel, &content);
            if !ct_eq(digest.as_bytes(), entry.hmac.as_bytes()) {
                return Err(SecretError::Tampered { path });
            }
            let hmac_digest = to_wire_digest(&digest)
                .ok_or_else(|| SecretError::Format("unexpected digest form".into()))?;
            files.push(SecretFile {
                path,
                hmac_digest,
                content: Bytes(content),
            });
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        check_paths(files.iter().map(|f| f.path.as_str())).map_err(SecretError::BadPath)?;
        let sizes: Vec<(usize, usize)> = files
            .iter()
            .map(|f| (f.path.len(), f.content.0.len()))
            .collect();
        check_limits(&sizes).map_err(SecretError::Limit)?;
        Ok(files)
    }

    pub(crate) fn claim_for_retention(
        &self,
        lease: &Lease,
        run: &RunId,
        entries: &[SecretEntry],
        key: &ClusterKey,
    ) -> anyhow::Result<Claim> {
        // HMAC and file identity must describe the very same bytes. A later
        // replacement cannot become authority merely by being privately owned.
        let (claim, bytes) = lease.proof(run)?;
        if let Some(bytes) = bytes {
            anyhow::ensure!(
                !entries.is_empty(),
                "unexpected Vault bundle for a secret-free run"
            );
            Self::decode(&bytes, entries, key)?;
        }
        Ok(claim)
    }

    /// Delete the run's bundle (idempotent).
    pub fn erase(&self, run: &RunId) -> Result<()> {
        // Even unregistered worker erasure participates in SH exclusion: a
        // parent cannot register/claim concurrently with this ephemeral erase.
        let shared = self.lease(false)?;
        let registered = shared
            .registered()
            .map_err(|e| SecretError::io("read Vault registration", std::io::Error::other(e)))?;
        let lease = if registered {
            drop(shared);
            self.lease(true)?
        } else {
            shared
        };
        self.available(&lease, run)?;
        let result = (|| {
            let claim = lease.claim(run)?;
            lease.drain(run, &claim)
        })();
        result.map_err(|e| SecretError::io("erase private Vault bundle", std::io::Error::other(e)))
    }

    /// Delete the bundle of every run `is_terminal` says is finished (and
    /// whose required collection is done), and the stale temporary files of
    /// the directory. Returns the runs erased. A file that is not a run's
    /// bundle is left alone.
    pub fn sweep(&self, is_terminal: impl Fn(&RunId) -> bool) -> Result<Vec<RunId>> {
        sweep_stale_tmp(&self.dir, STALE_TMP_AGE)?;
        let mut erased = Vec::new();
        let rd = fs::read_dir(&self.dir)
            .map_err(|e| SecretError::io(format!("directory {}", self.dir.display()), e))?;
        for entry in rd {
            let entry = entry
                .map_err(|e| SecretError::io(format!("directory {}", self.dir.display()), e))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(stem) = name.strip_suffix(".json") else {
                continue;
            };
            let Ok(run) = RunId::parse(stem) else {
                continue;
            };
            if is_terminal(&run) {
                self.erase(&run)?;
                erased.push(run);
            }
        }
        erased.sort();
        Ok(erased)
    }
}
