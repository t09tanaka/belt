//! Staged publication of a capture (opt-in).
//!
//! The default entry points ([`capture_source`](super::capture_source),
//! [`capture_directory`](super::capture_directory)) write each object
//! straight into the shared store while they read, before they know the input
//! held still. A caller that has to account for every file a capture creates
//! (to own it, or to remove it after a crash) cannot, with those: an attempt
//! that turns out unstable leaves its objects, and a caller killed between the
//! return and its own bookkeeping does not know which objects the capture
//! made.
//!
//! The staged entry points ([`capture_source_staged`](super::capture_source_staged),
//! [`capture_directory_staged`](super::capture_directory_staged)) write every
//! new file of the store (objects, secret bundle, manifest, and the temporary
//! files that go under the store's `tmp/` otherwise) under a directory the
//! caller names, on the store's file system. Nothing enters the store until
//! the capture is stable and the manifest sealed; then the caller's
//! `before_install` hook sees the full list of what is about to be installed
//! (once), and each file is installed without replacing anything.
//!
//! Out of scope: the private Git repository under `git/<key>.git` (objects and
//! refs) is written as before; it has a life cycle of its own.

use super::store::{write_atomic, ObjectStore};
use super::SourceError;
use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The caller's hook, see [`Staging::before_install`].
pub type BeforeInstall = Arc<dyn Fn(&[Intent]) -> Result<()> + Send + Sync>;

/// What the caller supplies to make a capture staged.
#[derive(Clone)]
pub struct Staging {
    /// An existing directory on the file system of the store. Local-ci makes
    /// `objects/`, `manifests/`, `secrets/` and `tmp/` in it and removes the
    /// files it staged; the directory itself is the caller's.
    pub scratch_dir: PathBuf,
    /// Called once, after the capture is stable and the manifest sealed, with
    /// everything that is about to be installed in the store. An error stops
    /// the capture: nothing is installed and the staged files are removed.
    pub before_install: BeforeInstall,
}

impl std::fmt::Debug for Staging {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Staging")
            .field("scratch_dir", &self.scratch_dir)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentKind {
    /// `objects/<aa>/<hash>`
    Object { hash: String, size: u64 },
    /// `manifests/<source_id>.json`; `digest` is its `manifest_digest`. A
    /// capture with submodules has one per submodule too.
    Manifest { source_id: String, digest: String },
    /// `secrets/<source_id>.json`, only when the bundle goes to the store
    /// (not when `CaptureOptions::secret_bundle` names a path).
    SecretBundle { source_id: String },
}

/// One file that is about to be installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    pub kind: IntentKind,
    /// Where it is now, under the scratch directory.
    pub staged: PathBuf,
    /// Where it will be in the store.
    pub destination: PathBuf,
}

/// The result of a staged capture.
#[derive(Debug, Clone)]
pub struct StagedSnapshot {
    pub snapshot: super::SourceSnapshot,
    /// Destinations this capture created: the content was not there.
    pub installed: Vec<PathBuf>,
    /// Destinations that were there already, with the same content (checked
    /// in full); the staged copy was dropped. They are not this capture's to
    /// remove.
    pub reused: Vec<PathBuf>,
}

#[derive(Debug)]
enum Item {
    Object {
        hash: String,
        size: u64,
        path: PathBuf,
    },
    Manifest {
        source_id: String,
        digest: String,
        path: PathBuf,
    },
    Secret {
        source_id: String,
        path: PathBuf,
    },
}

impl Item {
    fn path(&self) -> &Path {
        match self {
            Item::Object { path, .. } | Item::Manifest { path, .. } | Item::Secret { path, .. } => {
                path
            }
        }
    }
}

#[derive(Debug, Default)]
struct State {
    items: Vec<Item>,
    objects: HashMap<String, PathBuf>,
}

#[derive(Debug)]
pub(crate) struct Stager {
    scratch: PathBuf,
    state: Mutex<State>,
}

fn dev_of(p: &Path) -> std::io::Result<u64> {
    Ok(std::fs::metadata(p)?.dev())
}

/// `scratch` has to be on the file system of `store`: files are installed by
/// a link, which does not cross one. `dev` says which file system a path is
/// on.
pub(crate) fn check_same_filesystem(
    scratch: &Path,
    store: &Path,
    dev: &dyn Fn(&Path) -> std::io::Result<u64>,
) -> Result<()> {
    let a = dev(scratch).with_context(|| format!("cannot stat {}", scratch.display()))?;
    let b = dev(store).with_context(|| format!("cannot stat {}", store.display()))?;
    if a != b {
        bail!(
            "the scratch directory {} is not on the file system of the source store {}: staged files are installed by a link",
            scratch.display(),
            store.display()
        );
    }
    Ok(())
}

impl Stager {
    pub(crate) fn new(staging: &Staging, store: &ObjectStore) -> Result<Arc<Stager>> {
        Self::with_dev(staging, store, &dev_of)
    }

    pub(crate) fn with_dev(
        staging: &Staging,
        store: &ObjectStore,
        dev: &dyn Fn(&Path) -> std::io::Result<u64>,
    ) -> Result<Arc<Stager>> {
        let scratch = &staging.scratch_dir;
        if !std::fs::metadata(scratch)
            .with_context(|| format!("scratch directory {}", scratch.display()))?
            .is_dir()
        {
            bail!("scratch directory {} is not a directory", scratch.display());
        }
        check_same_filesystem(scratch, store.root(), dev)?;
        let scratch = std::fs::canonicalize(scratch)?;
        for d in ["objects", "manifests", "secrets", "tmp"] {
            let p = scratch.join(d);
            std::fs::create_dir_all(&p)?;
            std::fs::set_permissions(&p, std::os::unix::fs::PermissionsExt::from_mode(0o700))?;
        }
        Ok(Arc::new(Stager {
            scratch,
            state: Mutex::new(State::default()),
        }))
    }

    pub(crate) fn tmp_dir(&self) -> PathBuf {
        self.scratch.join("tmp")
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Where the staged object is, when this capture staged it.
    pub(crate) fn object_path(&self, hash: &str) -> Option<PathBuf> {
        self.lock().objects.get(hash).cloned()
    }

    /// Stage what `r` yields; returns its hash and size. The bytes are
    /// written even when the store has the object: whether it is a hit is for
    /// the install to say, with the content in hand.
    pub(crate) fn put_reader(&self, r: &mut impl Read) -> Result<(String, u64)> {
        let tmp = super::store::tmp_name(&self.tmp_dir(), "obj");
        let res = (|| -> Result<(String, u64)> {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&tmp)?;
            let mut h = Sha256::new();
            let mut n = 0u64;
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let k = r.read(&mut buf)?;
                if k == 0 {
                    break;
                }
                h.update(&buf[..k]);
                f.write_all(&buf[..k])?;
                n += k as u64;
            }
            f.sync_all()?;
            let hash = super::store::hex(&h.finalize());
            let mut st = self.lock();
            if st.objects.contains_key(&hash) {
                std::fs::remove_file(&tmp)?;
            } else {
                let path = self.scratch.join("objects").join(&hash);
                // the scratch directory is ours: a leftover of that name
                // from a dead process is replaced, never trusted
                std::fs::rename(&tmp, &path)?;
                st.objects.insert(hash.clone(), path.clone());
                st.items.push(Item::Object {
                    hash: hash.clone(),
                    size: n,
                    path,
                });
            }
            Ok((hash, n))
        })();
        if res.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        res
    }

    pub(crate) fn stage_manifest(&self, m: &super::SourceManifest) -> Result<()> {
        let path = self
            .scratch
            .join("manifests")
            .join(format!("{}.json", m.source_id));
        write_atomic(&path, &serde_json::to_vec_pretty(m)?, 0o644)?;
        let mut st = self.lock();
        st.items.retain(|i| i.path() != path);
        st.items.push(Item::Manifest {
            source_id: m.source_id.clone(),
            digest: m.manifest_digest.clone(),
            path,
        });
        Ok(())
    }

    pub(crate) fn stage_secrets(
        &self,
        source_id: &str,
        bundle: &super::SecretBundle,
    ) -> Result<()> {
        let path = self
            .scratch
            .join("secrets")
            .join(format!("{source_id}.json"));
        bundle.save(&path)?;
        let mut st = self.lock();
        st.items.retain(|i| i.path() != path);
        st.items.push(Item::Secret {
            source_id: source_id.to_string(),
            path,
        });
        Ok(())
    }

    /// What has been staged so far.
    pub(crate) fn mark(&self) -> usize {
        self.lock().items.len()
    }

    /// Remove what was staged after `mark` (a failed attempt's files).
    pub(crate) fn reset_to(&self, mark: usize) {
        let mut st = self.lock();
        while st.items.len() > mark {
            let Some(item) = st.items.pop() else { break };
            if let Item::Object { hash, .. } = &item {
                st.objects.remove(hash);
            }
            let _ = std::fs::remove_file(item.path());
        }
    }

    /// Install what was staged: objects, then secret bundles, then manifests
    /// (a manifest that is there is complete). `before_install` sees the list
    /// first; an error from it installs nothing.
    pub(crate) fn install(
        &self,
        store: &ObjectStore,
        before_install: &(dyn Fn(&[Intent]) -> Result<()> + Send + Sync),
    ) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
        let intents = self.intents(store);
        before_install(&intents)?;
        let mut installed = vec![];
        let mut reused = vec![];
        let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
        for i in &intents {
            let dest = &i.destination;
            let parent = dest.parent().context("no parent directory")?;
            if !parent.is_dir() {
                std::fs::create_dir_all(parent)?;
                if let Some(grand) = parent.parent() {
                    dirs.insert(grand.to_path_buf());
                }
            }
            dirs.insert(parent.to_path_buf());
            match std::fs::hard_link(&i.staged, dest) {
                Ok(()) => installed.push(dest.clone()),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    verify_existing(&i.kind, &i.staged, dest)?;
                    reused.push(dest.clone());
                }
                Err(e) => {
                    return Err(anyhow::Error::from(e).context(format!(
                        "cannot install {} (the scratch directory must be on the file system of the store, with hard links)",
                        dest.display()
                    )))
                }
            }
            std::fs::remove_file(&i.staged)?;
        }
        for d in dirs {
            std::fs::File::open(&d)
                .and_then(|f| f.sync_all())
                .with_context(|| format!("cannot sync {}", d.display()))?;
        }
        let mut st = self.lock();
        st.items.clear();
        st.objects.clear();
        Ok((installed, reused))
    }

    fn intents(&self, store: &ObjectStore) -> Vec<Intent> {
        let st = self.lock();
        let mut out = vec![];
        let rank = |want: u8, out: &mut Vec<Intent>| {
            for item in &st.items {
                match (want, item) {
                    (0, Item::Object { hash, size, path }) => out.push(Intent {
                        kind: IntentKind::Object {
                            hash: hash.clone(),
                            size: *size,
                        },
                        staged: path.clone(),
                        destination: store.object_path(hash),
                    }),
                    (1, Item::Secret { source_id, path }) => out.push(Intent {
                        kind: IntentKind::SecretBundle {
                            source_id: source_id.clone(),
                        },
                        staged: path.clone(),
                        destination: store.secrets_path(source_id),
                    }),
                    (
                        2,
                        Item::Manifest {
                            source_id,
                            digest,
                            path,
                        },
                    ) => out.push(Intent {
                        kind: IntentKind::Manifest {
                            source_id: source_id.clone(),
                            digest: digest.clone(),
                        },
                        staged: path.clone(),
                        destination: store.manifest_path(source_id),
                    }),
                    _ => {}
                }
            }
        };
        for want in 0..3 {
            rank(want, &mut out);
        }
        out
    }
}

fn file_digest(p: &Path) -> Result<String> {
    let mut f = std::fs::File::open(p)?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let k = f.read(&mut buf)?;
        if k == 0 {
            break;
        }
        h.update(&buf[..k]);
    }
    Ok(super::store::hex(&h.finalize()))
}

/// The destination exists: it is a regular file with the content the intent
/// stands for (the whole of it is read), or an error. Nothing is replaced.
fn verify_existing(kind: &IntentKind, staged: &Path, dest: &Path) -> Result<()> {
    let corrupt = |what: String| -> anyhow::Error { SourceError::Corrupt { what }.into() };
    let meta = std::fs::symlink_metadata(dest)?;
    if !meta.is_file() {
        return Err(corrupt(format!(
            "{} exists and is not a regular file; it was not replaced",
            dest.display()
        )));
    }
    match kind {
        IntentKind::Object { hash, .. } => {
            let have = file_digest(dest)?;
            if &have != hash {
                return Err(corrupt(format!(
                    "{} has the content {have}, not {hash}; it was not replaced",
                    dest.display()
                )));
            }
        }
        IntentKind::Manifest { digest, .. } => {
            let m: super::SourceManifest = serde_json::from_slice(&std::fs::read(dest)?)
                .with_context(|| format!("{} is not a manifest", dest.display()))?;
            super::capture::check_digest(&m)?;
            if &m.manifest_digest != digest {
                return Err(corrupt(format!(
                    "{} is the manifest {}, not {digest}; it was not replaced",
                    dest.display(),
                    m.manifest_digest
                )));
            }
        }
        IntentKind::SecretBundle { .. } => {
            // the bundle is a function of the source (its digest is in the
            // manifest), so an existing one has to equal the staged one
            let (have, want) = (file_digest(dest)?, file_digest(staged)?);
            if have != want {
                return Err(corrupt(format!(
                    "{} differs from the secret bundle of this capture; it was not replaced",
                    dest.display()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn staging(scratch: &Path) -> Staging {
        Staging {
            scratch_dir: scratch.to_path_buf(),
            before_install: Arc::new(|_| Ok(())),
        }
    }

    #[test]
    fn a_scratch_directory_on_another_file_system_is_refused_and_nothing_is_made_in_it() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path().join("store")).unwrap();
        let scratch = tmp.path().join("scratch");
        std::fs::create_dir(&scratch).unwrap();
        // inject two devices: the real ones are the same here
        let other = scratch.clone();
        let dev = move |p: &Path| -> std::io::Result<u64> { Ok(if p == other { 2 } else { 1 }) };
        let err = Stager::with_dev(&staging(&scratch), &store, &dev).unwrap_err();
        assert!(format!("{err}").contains("not on the file system"), "{err}");
        assert!(std::fs::read_dir(&scratch).unwrap().next().is_none());
        // the same device is accepted
        assert!(Stager::with_dev(&staging(&scratch), &store, &|_| Ok(1)).is_ok());
        // and so is the real check, on one file system
        assert!(Stager::new(&staging(&scratch), &store).is_ok());
    }
}
