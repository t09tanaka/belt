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
//! One scratch directory serves one capture at a time: the staged names are
//! content-derived and a capture clears what it staged, so two captures in
//! the same directory would remove each other's files. A capture holds an
//! exclusive `flock` on the scratch directory itself while it runs (nothing is
//! created for it, and it is released when the process ends, killed or not);
//! a second capture in a directory that is in use is refused.
//!
//! Out of scope: the private Git repository under `git/<key>.git` (objects and
//! refs) is written as before; it has a life cycle of its own. So is a secret
//! bundle at the path of `CaptureOptions::secret_bundle`: the caller named
//! that path, so it already knows it, and it is written before
//! `before_install` (a caller that is killed removes that path, which
//! `save_new` guaranteed did not exist; see `CaptureOptions::secret_bundle`).

use super::store::{write_atomic, ObjectStore};
use super::SourceError;
use anyhow::{bail, Context, Result};
use fs2::FileExt;
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
    /// files it staged; the directory itself is the caller's. It serves one
    /// capture at a time: a capture holds an exclusive `flock` on it while it
    /// runs, and a second one in the same directory is refused.
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

/// An install that stopped part-way. It is attached as context to the error
/// of the install (`err.downcast_ref::<StagedInstallError>()`; the error
/// underneath, such as [`SourceError::Corrupt`], can be downcast as well).
///
/// What was installed is not undone: another capture may be using those files
/// already, and they are the content they say they are. The caller learns what
/// this capture created (`installed`, to account for or remove) and what was
/// there already (`reused`, not its to remove); the files that were not
/// reached are not in the store and the staged ones are removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedInstallError {
    /// Destinations this capture created before the failure.
    pub installed: Vec<PathBuf>,
    /// Destinations that were there already, with the same content.
    pub reused: Vec<PathBuf>,
}

impl std::fmt::Display for StagedInstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the install stopped part-way: {} file(s) installed (kept), {} reused",
            self.installed.len(),
            self.reused.len()
        )
    }
}

impl std::error::Error for StagedInstallError {}

/// One step of an install, for a test that checks their order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    Link(PathBuf),
    SyncDir(PathBuf),
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
    /// The `flock` on the scratch directory, held while the capture runs.
    lock: std::fs::File,
}

impl Drop for Stager {
    /// Release the `flock` explicitly instead of only closing the file: the
    /// lock belongs to the open file description, and a child forked by any
    /// thread of the process keeps a copy of it until its `exec`. Closing our
    /// descriptor alone would leave the directory locked for that long, and
    /// the next capture in it would be refused as "in use".
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.lock);
    }
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
        let lock = std::fs::File::open(&scratch)
            .with_context(|| format!("cannot open scratch directory {}", scratch.display()))?;
        if let Err(e) = lock.try_lock_exclusive() {
            if e.raw_os_error() == fs2::lock_contended_error().raw_os_error() {
                bail!(
                    "the scratch directory {} is in use by another capture: one scratch directory serves one capture at a time",
                    scratch.display()
                );
            }
            return Err(anyhow::Error::from(e).context(format!(
                "cannot lock scratch directory {}",
                scratch.display()
            )));
        }
        for d in ["objects", "manifests", "secrets", "tmp"] {
            let p = scratch.join(d);
            std::fs::create_dir_all(&p)?;
            std::fs::set_permissions(&p, std::os::unix::fs::PermissionsExt::from_mode(0o700))?;
        }
        Ok(Arc::new(Stager {
            scratch,
            state: Mutex::new(State::default()),
            lock,
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
        let mut st = self.lock();
        // the name is derived from the content: one that is staged is this
        // one, and moving it to the end would shift the marks of the attempts
        if st.items.iter().any(|i| i.path() == path) {
            return Ok(());
        }
        write_atomic(&path, &serde_json::to_vec_pretty(m)?, 0o644)?;
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
        let mut st = self.lock();
        if st.items.iter().any(|i| i.path() == path) {
            return Ok(());
        }
        bundle.save(&path)?;
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
    /// first; an error from it installs nothing. An error part-way carries
    /// what was installed and reused so far ([`StagedInstallError`]).
    pub(crate) fn install(
        &self,
        store: &ObjectStore,
        before_install: &(dyn Fn(&[Intent]) -> Result<()> + Send + Sync),
    ) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
        let intents = self.intents(store);
        before_install(&intents)?;
        self.install_intents(&intents, &mut |_| {})
    }

    /// The install of `intents`, `observe` told of each link and each sync.
    ///
    /// Durability: the objects and the secret bundle are linked first, their
    /// directories (and the parent of one that was made, or `objects/` for an
    /// `objects/<aa>` that anyone made) are synced, and only then are the
    /// manifests linked and `manifests/` synced. A manifest that survives a
    /// crash therefore names objects that survive it too.
    fn install_intents(
        &self,
        intents: &[Intent],
        observe: &mut dyn FnMut(Step),
    ) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
        let mut installed = vec![];
        let mut reused = vec![];
        let res = (|| -> Result<()> {
            for manifests in [false, true] {
                let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
                for i in intents
                    .iter()
                    .filter(|i| matches!(i.kind, IntentKind::Manifest { .. }) == manifests)
                {
                    link_one(i, &mut installed, &mut reused, &mut dirs, observe)?;
                }
                for d in dirs {
                    observe(Step::SyncDir(d.clone()));
                    std::fs::File::open(&d)
                        .and_then(|f| f.sync_all())
                        .with_context(|| format!("cannot sync {}", d.display()))?;
                }
            }
            Ok(())
        })();
        match res {
            Ok(()) => {
                let mut st = self.lock();
                st.items.clear();
                st.objects.clear();
                Ok((installed, reused))
            }
            Err(e) => Err(e.context(StagedInstallError { installed, reused })),
        }
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

/// Install one file: a link that does not replace; an existing destination is
/// verified and reused. The staged name is removed afterwards.
fn link_one(
    i: &Intent,
    installed: &mut Vec<PathBuf>,
    reused: &mut Vec<PathBuf>,
    dirs: &mut BTreeSet<PathBuf>,
    observe: &mut dyn FnMut(Step),
) -> Result<()> {
    let dest = &i.destination;
    let parent = dest.parent().context("no parent directory")?;
    let made = !parent.is_dir();
    if made {
        std::fs::create_dir_all(parent)?;
    }
    dirs.insert(parent.to_path_buf());
    if let Some(grand) = parent.parent() {
        // `objects/` holds an `objects/<aa>` that this or any capture made
        if made || matches!(i.kind, IntentKind::Object { .. }) {
            dirs.insert(grand.to_path_buf());
        }
    }
    observe(Step::Link(dest.clone()));
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
    Ok(())
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

    /// A child forked while a capture held the scratch directory (here one
    /// that waits between fork and exec) does not keep it locked after the
    /// capture ends.
    #[test]
    #[allow(clippy::disallowed_methods)]
    fn a_child_forked_during_a_capture_does_not_keep_the_scratch_directory_locked() {
        use std::os::unix::process::CommandExt;
        fn pipe() -> (i32, i32) {
            let mut fds = [0; 2];
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            for fd in fds {
                assert_eq!(
                    unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) },
                    0
                );
            }
            (fds[0], fds[1])
        }
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path().join("store")).unwrap();
        let scratch = tmp.path().join("scratch");
        std::fs::create_dir(&scratch).unwrap();
        let first = Stager::new(&staging(&scratch), &store).unwrap();
        let (ready_r, ready_w) = pipe();
        let (go_r, go_w) = pipe();
        let spawner = std::thread::spawn(move || {
            let mut cmd = std::process::Command::new("true");
            unsafe {
                cmd.pre_exec(move || {
                    let b = [1u8];
                    libc::write(ready_w, b.as_ptr().cast(), 1);
                    let mut r = [0u8];
                    libc::read(go_r, r.as_mut_ptr().cast(), 1);
                    Ok(())
                });
            }
            cmd.spawn().unwrap()
        });
        // the child is forked and holds a copy of every descriptor
        let mut b = [0u8];
        assert_eq!(unsafe { libc::read(ready_r, b.as_mut_ptr().cast(), 1) }, 1);
        drop(first);
        let again = Stager::new(&staging(&scratch), &store);
        // let the child exec and reap it before asserting
        assert_eq!(unsafe { libc::write(go_w, [1u8].as_ptr().cast(), 1) }, 1);
        let mut child = spawner.join().unwrap();
        assert!(child.wait().unwrap().success());
        for fd in [ready_r, ready_w, go_r, go_w] {
            unsafe { libc::close(fd) };
        }
        let again = again.unwrap();
        // and the directory is still refused to a second capture while held
        let err = Stager::new(&staging(&scratch), &store).unwrap_err();
        assert!(
            format!("{err}").contains("in use by another capture"),
            "{err}"
        );
        drop(again);
        assert!(Stager::new(&staging(&scratch), &store).is_ok());
    }

    use crate::source::{
        CanonicalConfig, ObjectManifest, SecretBundle, SourceKind, SourceManifest,
    };
    use crate::source::{RelPath, SCHEMA_VERSION};

    fn manifest(id: &str) -> SourceManifest {
        SourceManifest {
            schema_version: SCHEMA_VERSION,
            source_id: id.to_string(),
            manifest_digest: format!("sha256:{id}"),
            repository_id: None,
            source_kind: SourceKind::Directory,
            original_head: None,
            index_tree: None,
            worktree_tree: None,
            execution_commit: None,
            base: None,
            merge_base: None,
            branch: None,
            cwd_relative: RelPath(vec![]),
            object_manifest: ObjectManifest {
                entries: vec![],
                tree_digest: String::new(),
                git_tips: vec![],
            },
            submodules: vec![],
            lfs: vec![],
            canonical: CanonicalConfig::default(),
            secret_files: vec![],
        }
    }

    fn setup() -> (tempfile::TempDir, ObjectStore, Arc<Stager>) {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path().join("store")).unwrap();
        let scratch = tmp.path().join("scratch");
        std::fs::create_dir(&scratch).unwrap();
        let stager = Stager::new(&staging(&scratch), &store).unwrap();
        (tmp, store, stager)
    }

    #[test]
    fn staging_the_same_name_again_does_not_move_it_past_the_mark_of_an_attempt() {
        let (_tmp, _store, stager) = setup();
        let bundle = SecretBundle::new(vec![(RelPath::new("s"), 0o600, b"x".to_vec())]);
        stager.stage_manifest(&manifest("src-one")).unwrap();
        stager.stage_secrets("src-one", &bundle).unwrap();
        let mark = stager.mark();
        assert_eq!(mark, 2);
        let (hash, _) = stager.put_reader(&mut &b"attempt"[..]).unwrap();
        // the same names again, within the attempt: nothing is staged
        stager.stage_manifest(&manifest("src-one")).unwrap();
        stager.stage_secrets("src-one", &bundle).unwrap();
        assert_eq!(stager.mark(), 3);
        // so the reset to the mark removes the attempt's object and only that
        stager.reset_to(mark);
        assert!(stager.object_path(&hash).is_none());
        assert!(stager.scratch.join("manifests/src-one.json").is_file());
        assert!(stager.scratch.join("secrets/src-one.json").is_file());
        assert_eq!(stager.mark(), 2);
    }

    #[test]
    fn the_directories_of_objects_and_secrets_are_synced_before_a_manifest_is_linked() {
        let (_tmp, store, stager) = setup();
        stager.put_reader(&mut &b"one"[..]).unwrap();
        stager.put_reader(&mut &b"two"[..]).unwrap();
        stager
            .stage_secrets(
                "src-a",
                &SecretBundle::new(vec![(RelPath::new("s"), 0o600, b"x".to_vec())]),
            )
            .unwrap();
        stager.stage_manifest(&manifest("src-a")).unwrap();
        let intents = stager.intents(&store);
        let mut steps = vec![];
        let (installed, reused) = stager
            .install_intents(&intents, &mut |s| steps.push(s))
            .unwrap();
        assert_eq!(installed.len(), 4);
        assert!(reused.is_empty());
        let pos = |want: &Step| steps.iter().position(|s| s == want).unwrap();
        let manifest_dest = store.manifest_path("src-a");
        let first_manifest_link = pos(&Step::Link(manifest_dest.clone()));
        // every object and the secret bundle is linked, and every directory
        // that holds one synced, before the manifest is linked
        for i in intents
            .iter()
            .filter(|i| !matches!(i.kind, IntentKind::Manifest { .. }))
        {
            assert!(pos(&Step::Link(i.destination.clone())) < first_manifest_link);
            let dir = i.destination.parent().unwrap().to_path_buf();
            assert!(pos(&Step::SyncDir(dir)) < first_manifest_link);
        }
        // `objects/` itself too (its `<aa>` was made by this install), and
        // `secrets/`
        assert!(pos(&Step::SyncDir(store.root().join("objects"))) < first_manifest_link);
        assert!(pos(&Step::SyncDir(store.root().join("secrets"))) < first_manifest_link);
        // and `manifests/` last, after the manifest is linked
        let sync_manifests = pos(&Step::SyncDir(store.root().join("manifests")));
        assert!(first_manifest_link < sync_manifests);
        assert_eq!(sync_manifests, steps.len() - 1);
    }
}
