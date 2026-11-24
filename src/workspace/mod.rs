//! The worker's workspaces (spec 22.5, chapter 6).
//!
//! A repository has one canonical clone on the worker (where bundles are
//! received); every attempt of a Git source gets a worktree of it made by
//! `fast-worktree` at the source's execution commit (the project's
//! `.fast-worktree.toml` prepares the dependencies), then the verified files of
//! the source are laid over it. A directory snapshot gets a plain directory.
//! `exec --in-place` uses the user's own worktree and never removes it.
//!
//! All of it is blocking (Git, file I/O, child processes): call it from a
//! dedicated thread or `spawn_blocking`.
//!
//! Locks (per canonical clone, in `<clone>/.git`): adds of one commit share
//! the clone; moving its checkout to another commit is exclusive. A
//! writer-priority `intent` lock keeps a stream of adds of one commit from
//! starving an add of another: whoever wants the main lock passes through the
//! intent lock first, and a writer holds it while it waits for the readers to
//! drain. Removals and their branch cleanup use the same two locks.
//!
//! A repository that has a worker crash mid-way is repaired lazily: a create
//! of the same attempt clears what the dead one left, and
//! [`Workspaces::sweep_orphans`] (the worker's startup, S10) clears the
//! directories no record names.

pub mod embedded;
mod fastwt;
mod git;
pub mod in_place;
mod overlay;
pub mod plan;

pub use fastwt::{CliFastWorktree, FastWorktree, DEFAULT_TIMEOUT as DEFAULT_PREPARE_TIMEOUT};
pub use plan::FwFailure;

use crate::error::{ApiError, BlockedReason, ErrorCode};
use crate::ids::AttemptId;
use crate::platform::flock::HeldLock;
use crate::platform::owned_temp::OwnedTemp;
use crate::source::classify::api_error_of;
use crate::source::retention::CloneGc;
use crate::source::{ObjectsRepo, SourceStore};
use git::Env;
use local_ci::source::SourceManifest as CiManifest;
use plan::Flavor;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// Git removes this private per-worktree ref with the worktree registration.
// It keeps the admission snapshot reachable even after reproducing the original HEAD.
/// How long one git of step 4 may run before it is killed.
const REF_GIT_LIMIT: std::time::Duration = std::time::Duration::from_secs(10);
/// The bound of one step 4 row, all its git calls together.
const REF_ROW_LIMIT: std::time::Duration = std::time::Duration::from_secs(20);
/// The bound of one gc of (e) in a clone; one past it is ended, and the
/// clone's next gc is a day later.
const GC_LIMIT: std::time::Duration = std::time::Duration::from_secs(600);
/// The configuration of the clone gc's command line (review round 1, M2,
/// L3): no packing of refs (step 4 removes loose refs), no worktree prune,
/// no reflog expiry (it would take ref
/// locks in the linked worktrees: with both set to `never` gc skips its
/// reflog step), no auto gc or maintenance of its own.
const GC_CONFIG: [&str; 6] = [
    "gc.packRefs=false",
    "gc.worktreePruneExpire=never",
    "gc.reflogExpire=never",
    "gc.reflogExpireUnreachable=never",
    "gc.auto=0",
    "maintenance.auto=false",
];
/// For belt's gits that move the main checkout's HEAD (review round 1, M1).
const NO_REFLOG: &str = "core.logAllRefUpdates=false";
const CLEANUP_COMMIT: &str = "refs/worktree/belt-cleanup";

/// What kind of workspace this is, and so how it goes away.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceKind {
    /// A worktree of the canonical clone `clone`, on `branch`.
    Worktree { clone: PathBuf, branch: String },
    /// A plain directory the runner made.
    Directory,
    /// The user's own worktree: never removed, never copied.
    InPlace,
}

/// A prepared workspace: where the run's files are, and where it starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workspace {
    pub root: PathBuf,
    pub cwd: PathBuf,
    pub kind: WorkspaceKind,
    /// `git diff` and `git diff --cached` in the workspace show what the
    /// original's did (spec 6): the branch is at the original's HEAD and the
    /// index is the original's. `false` when the clone lacked the object to
    /// do it (see `warnings`); `true` when nothing was lost, which is also
    /// what a directory or in-place workspace says.
    pub index_reproduced: bool,
    /// What the supervisor reports as events: things that did not stop the
    /// workspace from being made but a user should know.
    #[serde(default)]
    pub warnings: Vec<String>,
}

impl Workspace {
    fn plain(root: PathBuf, cwd: PathBuf, kind: WorkspaceKind) -> Self {
        Workspace {
            root,
            cwd,
            kind,
            index_reproduced: true,
            warnings: Vec::new(),
        }
    }
}

/// The worker's workspaces under its state directory: `clones/` holds the
/// canonical clones, `workspaces/` one directory per attempt.
pub struct Workspaces {
    state_dir: PathBuf,
    env: Env,
    fw: Arc<dyn FastWorktree>,
}

fn internal(e: anyhow::Error) -> ApiError {
    api_error_of(&e)
}

fn io_err(what: &str, e: std::io::Error) -> ApiError {
    ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

impl Workspaces {
    pub fn new(state_dir: PathBuf, env: Env, fw: Arc<dyn FastWorktree>) -> Self {
        Workspaces { state_dir, env, fw }
    }

    /// What a worker asks of its Git before it serves anything: that it
    /// runs, and is 2.31 or newer (`git fetch --atomic`). An older one would fail
    /// every bundle it is sent, whatever the bundle.
    pub fn check_git(&self) -> Result<(), ApiError> {
        git::check_version(&self.env).map_err(|e| {
            ApiError::blocked(
                BlockedReason::EnvironmentMismatch,
                format!("this worker cannot start: {e:#}"),
            )
        })
    }

    pub fn workspaces_root(&self) -> PathBuf {
        self.state_dir.join("workspaces")
    }

    pub fn clones_root(&self) -> PathBuf {
        self.state_dir.join("clones")
    }

    /// The canonical clone of `repository_id`, made when it is not there yet:
    /// empty (what a worker does: its clone has no `origin`, and bundles bring
    /// all it holds), or cloned from `seed` (a path or URL of the
    /// repository). An existing clone must have
    /// been made from the same `seed` (`Conflict` otherwise: its objects and
    /// its `origin` would be another repository's). Safe to call from several
    /// threads and processes at once.
    pub fn ensure_clone(
        &self,
        repository_id: &str,
        seed: Option<&str>,
    ) -> Result<ObjectsRepo, ApiError> {
        let root = self.clones_root();
        std::fs::create_dir_all(&root)
            .map_err(|e| io_err("cannot create the clones directory", e))?;
        let name = plan::clone_dir_name(repository_id);
        let dir = root.join(&name);
        let lock = open_lock(&root.join(format!("{name}.lock")))?;
        lock.lock()
            .map_err(|e| io_err("cannot lock the clone", e))?;
        if dir.join(".git").exists() {
            let have = git::run(&self.env, &dir, &["remote", "get-url", "origin"]).ok();
            let same = match (seed, have.as_deref()) {
                (None, None) => true,
                (Some(s), Some(h)) => same_location(s, h),
                _ => false,
            };
            if !same {
                return Err(ApiError::new(
                    ErrorCode::Conflict,
                    format!(
                        "the canonical clone of {repository_id} was made from {have:?}, not from {seed:?}"
                    ),
                ));
            }
        } else {
            // build beside, move into place: a crash leaves no half clone
            let tmp = OwnedTemp::new_in(&root, ".clone.")
                .map_err(|e| io_err("cannot make a scratch directory", e))?;
            let work = tmp.path().join("c");
            match seed {
                Some(s) => {
                    let w = work.to_string_lossy();
                    // (M1: no reflog of the main HEAD or branch: it would keep
                    // objects past the gc of spec 25.8 5b (e))
                    git::run(
                        &self.env,
                        tmp.path(),
                        &["-c", NO_REFLOG, "clone", "-q", "--", s, &w],
                    )
                    .map_err(internal)?
                }
                None => {
                    std::fs::create_dir(&work).map_err(|e| io_err("cannot create the clone", e))?;
                    git::run(&self.env, &work, &["init", "-q"]).map_err(internal)?
                }
            };
            // a leftover directory without `.git` is ours (we hold the lock)
            if std::fs::symlink_metadata(&dir).is_ok() {
                remove_tree(&dir).map_err(|e| io_err("cannot clear a broken clone", e))?;
            }
            std::fs::rename(&work, &dir).map_err(|e| io_err("cannot place the clone", e))?;
        }
        Ok(ObjectsRepo::new(self.env.clone(), dir))
    }

    /// The file whose lock keeps the imports of bundles into the clone of
    /// `repository_id` apart (opened, not locked: the caller decides how
    /// long it waits). Whoever unpacks a bundle into the clone holds it from
    /// before the clone is made until what the bundle brought is checked,
    /// and a clone left empty is removed. It is beside the clone, so it is
    /// there before the clone is, and it is not the lock [`Self::ensure_clone`]
    /// takes for itself.
    pub fn import_lock(&self, repository_id: &str) -> Result<File, ApiError> {
        let root = self.clones_root();
        std::fs::create_dir_all(&root)
            .map_err(|e| io_err("cannot create the clones directory", e))?;
        let name = plan::clone_dir_name(repository_id);
        open_lock(&root.join(format!("{name}.import.lock")))
    }

    /// The import lock of the clone `clone` (a name `r-<16 hex>`), tried
    /// without waiting: `None` when another holds it (review round 3, M-1:
    /// the `clone_ref` fill writes its rows under it).
    pub fn try_import_lock(&self, clone: &str) -> anyhow::Result<Option<HeldLock>> {
        anyhow::ensure!(
            clone.len() == 18
                && clone.starts_with("r-")
                && clone[2..]
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "not a clone name"
        );
        let root = self.clones_root();
        std::fs::create_dir_all(&root)?;
        let lock = open_lock(&root.join(format!("{clone}.import.lock")))
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        match lock.try_lock() {
            Ok(()) => Ok(Some(HeldLock::new(lock))),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
        }
    }

    /// Step 4 of the received pass (spec 25.8 5b (e)), called with no source
    /// lock held: under the import lock of the clone `clone` (a name
    /// `r-<16 hex>`) taken without waiting, unless `held()` (the source was
    /// received again), remove the refs belt's receiving put there for
    /// `source` (the snapshot and index refs, as local-ci's
    /// `remove_imported_refs` does, but with hooks off and a time bound),
    /// then `done()`. `false`: the lock is busy (an import is running),
    /// nothing was done. A clone that is not there has no ref to remove; one
    /// that cannot be looked at, or that (or whose `.git`) is not a real
    /// directory, is an error (the row stays). Git is bounded per call and
    /// for the row as a whole, and ended when `stop` says so (`false`: the
    /// row stays).
    pub fn drop_imported_refs(
        &self,
        clone: &str,
        source: &str,
        held: &dyn Fn() -> anyhow::Result<bool>,
        done: &dyn Fn() -> anyhow::Result<()>,
        stop: &dyn Fn() -> bool,
    ) -> anyhow::Result<bool> {
        anyhow::ensure!(
            clone.len() == 18
                && clone.starts_with("r-")
                && clone[2..]
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "not a clone name"
        );
        anyhow::ensure!(
            source.len() == 36
                && source.starts_with("src-")
                && source[4..]
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "not a source id"
        );
        let root = self.clones_root();
        let dir = root.join(clone);
        let git_dir = dir.join(".git");
        let lock = match std::fs::symlink_metadata(&dir) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
            Ok(meta) => {
                // (`symlink_metadata`: a symlink is not a directory)
                anyhow::ensure!(meta.is_dir(), "the clone is not a directory");
                anyhow::ensure!(
                    std::fs::symlink_metadata(&git_dir)?.is_dir(),
                    "the clone's .git is not a directory"
                );
                let lock = open_lock(&root.join(format!("{clone}.import.lock")))
                    .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                match lock.try_lock() {
                    Ok(()) => {}
                    Err(std::fs::TryLockError::WouldBlock) => return Ok(false),
                    Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
                }
                Some(HeldLock::new(lock))
            }
        };
        if lock.is_some() && !held()? && !self.remove_source_refs(&git_dir, source, stop)? {
            return Ok(false);
        }
        done()?;
        drop(lock);
        Ok(true)
    }

    /// With the import lock of the clone whose `.git` is `git_dir` held by
    /// the caller: remove the refs belt's receiving puts there for `source`
    /// (`refs/local-ci/{snapshots,index}/<source>`), each only when it is
    /// there, with the bounded git of step 4 (hooks off, no inherited
    /// `GIT_*`, its own process group, 10 s a call and 20 s in all).
    /// `false`: `stop` ended it.
    fn remove_source_refs(
        &self,
        git_dir: &Path,
        source: &str,
        stop: &dyn Fn() -> bool,
    ) -> anyhow::Result<bool> {
        let root = self.clones_root();
        let row_end = std::time::Instant::now() + REF_ROW_LIMIT;
        let git = |args: &[&str]| {
            let deadline = row_end.min(std::time::Instant::now() + REF_GIT_LIMIT);
            git::run_bounded(&self.env, git_dir, &root, args, deadline, stop)
        };
        for r in [
            format!("refs/local-ci/snapshots/{source}"),
            format!("refs/local-ci/index/{source}"),
        ] {
            let Some(there) = git(&["show-ref", "--verify", "-q", &r])? else {
                return Ok(false);
            };
            if there == 0 {
                let Some(code) = git(&["update-ref", "-d", &r])? else {
                    return Ok(false);
                };
                anyhow::ensure!(code == 0, "git update-ref -d {r} failed ({code})");
            } else {
                anyhow::ensure!(there == 1, "git show-ref {r} failed ({there})");
            }
        }
        Ok(true)
    }

    /// The gc of spec 25.8 5b (e) in the clone `clone` (a name
    /// `r-<16 hex>`), called with no source lock held, under the clone's
    /// import lock alone, tried without waiting (no unpack, step 4 or
    /// discard writes packs and refs while gc judges what is reachable).
    /// Its safety against fast-worktree, the workspaces' git and the users'
    /// own git in the worktrees rests on Git's 14 days of grace and on the
    /// worktrees' HEAD and index being reachable, not on a lock (review
    /// round 1, M2: `ensure_clone`, `hold_clone_at` and the teardown never
    /// wait for it). The configuration that would make gc touch more is
    /// pinned on its command line ([`GC_CONFIG`]); a clone with
    /// `extensions.preciousObjects` or a `gc.recentObjectsHook` set is not
    /// collected (`Refused`). Before it, the
    /// reflog of the main checkout's HEAD (only: never a linked worktree's)
    /// is expired, under the main lock tried without waiting (skipped while
    /// it is busy), so that what only an old checkout reached can go
    /// (M1). Its gits are the bounded ones of step 4 (no inherited `GIT_*`,
    /// hooks off, their own process group, SIGTERM then SIGKILL) with the
    /// bound [`GC_LIMIT`] for both, ended when `stop` says so. Lock files
    /// left in the clone are never removed. An error: a git failed, or ran
    /// past the bound.
    pub fn gc_clone(&self, clone: &str, stop: &dyn Fn() -> bool) -> anyhow::Result<CloneGc> {
        anyhow::ensure!(clone_name_ok(clone), "not a clone name");
        let root = self.clones_root();
        let dir = root.join(clone);
        let git_dir = dir.join(".git");
        match std::fs::symlink_metadata(&dir) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(CloneGc::Ran),
            Err(e) => return Err(e.into()),
            Ok(meta) => anyhow::ensure!(meta.is_dir(), "the clone is not a directory"),
        }
        anyhow::ensure!(
            std::fs::symlink_metadata(&git_dir)?.is_dir(),
            "the clone's .git is not a directory"
        );
        let open = |path: PathBuf| open_lock(&path).map_err(|e| anyhow::anyhow!("{e:?}"));
        let try_take =
            |f: File, take: fn(&File) -> Result<(), std::fs::TryLockError>| match take(&f) {
                Ok(()) => Ok(Some(HeldLock::new(f))),
                Err(std::fs::TryLockError::WouldBlock) => Ok(None),
                Err(std::fs::TryLockError::Error(e)) => Err(anyhow::Error::from(e)),
            };
        let Some(_importing) = try_take(
            open(root.join(format!("{clone}.import.lock")))?,
            File::try_lock,
        )?
        else {
            return Ok(CloneGc::Busy);
        };
        let deadline = std::time::Instant::now() + GC_LIMIT;
        let run =
            |args: &[&str]| git::run_bounded(&self.env, &git_dir, &root, args, deadline, stop);
        // (any value counts: kept is the safe side; a hook cannot be
        // unset on the command line, and is not run)
        let refused = r"^(extensions\.preciousobjects|gc\.recentobjectshook)$";
        match run(&["config", "--get-regexp", refused])? {
            None => return Ok(CloneGc::Stopped),
            Some(0) => return Ok(CloneGc::Refused),
            Some(1) => {}
            Some(code) => anyhow::bail!("git config failed ({code})"),
        }
        // (the main lock as `lock_exclusive` takes it, through its intent
        // lock, but tried: a checkout that waits for it is not stalled
        // longer than the expire takes)
        // (a clone belt made has no such reflog: nothing to expire)
        let logged =
            std::fs::symlink_metadata(git_dir.join("logs/HEAD")).is_ok_and(|m| m.is_file());
        let main = if !logged {
            None
        } else if let Some(_intent) =
            try_take(open(git_dir.join("belt-workspace.intent"))?, File::try_lock)?
        {
            try_take(open(git_dir.join("belt-workspace.lock"))?, File::try_lock)?
        } else {
            None
        };
        if let Some(main) = main {
            let expire = [
                "reflog",
                "expire",
                "--expire=now",
                "--expire-unreachable=now",
                "HEAD",
            ];
            // (its own short bound: the main lock is held meanwhile)
            let until = deadline.min(std::time::Instant::now() + REF_ROW_LIMIT);
            match git::run_bounded(&self.env, &git_dir, &root, &expire, until, stop)? {
                None => return Ok(CloneGc::Stopped),
                Some(0) => {}
                Some(code) => anyhow::bail!("git reflog expire failed ({code})"),
            }
            drop(main);
        }
        let mut args: Vec<&str> = Vec::new();
        for c in GC_CONFIG {
            args.extend(["-c", c]);
        }
        args.extend(["gc", "--quiet", "--prune=14.days.ago"]);
        match run(&args)? {
            None => Ok(CloneGc::Stopped),
            Some(0) => Ok(CloneGc::Ran),
            Some(code) => anyhow::bail!("git gc failed ({code})"),
        }
    }

    /// A scratch repository to try a bundle in before it touches the clone
    /// of `repository_id`: it has the refs of the clone and borrows its
    /// objects (nothing is copied, and nothing is written to the clone), or
    /// is empty when there is no clone yet. It is gone when the returned
    /// directory is dropped; one a worker that died left is cleared by
    /// [`Self::sweep_orphans`].
    pub fn rehearsal(&self, repository_id: &str) -> Result<(OwnedTemp, ObjectsRepo), ApiError> {
        let root = self.clones_root();
        std::fs::create_dir_all(&root)
            .map_err(|e| io_err("cannot create the clones directory", e))?;
        let clone = root.join(plan::clone_dir_name(repository_id));
        let tmp = OwnedTemp::new_in(&root, ".clone.")
            .map_err(|e| io_err("cannot make a scratch directory", e))?;
        let work = tmp.path().join("r");
        if clone.join(".git").exists() {
            let (from, to) = (clone.to_string_lossy(), work.to_string_lossy());
            git::run(
                &self.env,
                tmp.path(),
                &["clone", "-q", "--mirror", "--shared", "--", &from, &to],
            )
            .map_err(internal)?;
        } else {
            std::fs::create_dir(&work)
                .map_err(|e| io_err("cannot create the scratch repository", e))?;
            git::run(&self.env, &work, &["init", "-q"]).map_err(internal)?;
        }
        Ok((tmp, ObjectsRepo::new(self.env.clone(), work)))
    }

    /// Remove the clone of `repository_id` when it holds no ref: one that
    /// was made for a bundle that then did not unpack. A clone that holds
    /// anything stays. Returns whether a clone was removed.
    pub fn remove_clone_if_empty(&self, repository_id: &str) -> Result<bool, ApiError> {
        let root = self.clones_root();
        let name = plan::clone_dir_name(repository_id);
        let dir = root.join(&name);
        if !dir.join(".git").exists() {
            return Ok(false);
        }
        let lock = open_lock(&root.join(format!("{name}.lock")))?;
        lock.lock()
            .map_err(|e| io_err("cannot lock the clone", e))?;
        let refs = git::run(&self.env, &dir, &["for-each-ref", "--count=1"]).map_err(internal)?;
        if !refs.trim().is_empty() {
            return Ok(false);
        }
        remove_tree(&dir).map_err(|e| io_err("cannot remove an empty clone", e))?;
        Ok(true)
    }

    /// Bring the clone up to date with its `origin`, when it has one.
    pub fn update_clone(&self, clone: &ObjectsRepo) -> Result<(), ApiError> {
        let has_origin = git::run(&self.env, clone.dir(), &["remote"])
            .map_err(internal)?
            .lines()
            .any(|l| l == "origin");
        if has_origin {
            git::run(
                &self.env,
                clone.dir(),
                &["-c", NO_REFLOG, "fetch", "-q", "--no-tags", "origin"],
            )
            .map_err(internal)?;
        }
        Ok(())
    }

    /// The workspace of `attempt` for the source `m`. A Git source needs its
    /// canonical clone to hold the execution commit already (received by
    /// bundle); `repo` is that clone. On any failure nothing of the workspace
    /// is left behind.
    pub fn create(
        &self,
        attempt: &AttemptId,
        store: &SourceStore,
        m: &CiManifest,
        repo: Option<&ObjectsRepo>,
    ) -> Result<Workspace, ApiError> {
        let flavor = plan::flavor_of(m.execution_commit.as_deref(), m.repository_id.as_deref())?;
        let cwd_rel = m.cwd_relative.to_path();
        plan::check_cwd_relative(&cwd_rel)?;
        let root = self.workspaces_root();
        std::fs::create_dir_all(&root)
            .map_err(|e| io_err("cannot create the workspaces directory", e))?;
        let dest = plan::workspace_dir(&root, attempt);
        match flavor {
            Flavor::Directory => {
                std::fs::create_dir(&dest).map_err(|e| {
                    if e.kind() == std::io::ErrorKind::AlreadyExists {
                        ApiError::new(
                            ErrorCode::Conflict,
                            format!("the workspace of {attempt} already exists"),
                        )
                    } else {
                        io_err("cannot create the workspace", e)
                    }
                })?;
                let made = store.materialize(m, &dest).map_err(internal).map(|()| {
                    Workspace::plain(dest.clone(), dest.join(&cwd_rel), WorkspaceKind::Directory)
                });
                if made.is_err() {
                    let _ = remove_tree(&dest);
                }
                made
            }
            Flavor::Git { commit, .. } => {
                let repo = repo.ok_or_else(|| {
                    ApiError::new(
                        ErrorCode::InvalidInput,
                        "a Git source needs the repository's canonical clone",
                    )
                })?;
                self.create_git(attempt, store, m, repo, &commit, &dest)
            }
        }
    }

    fn create_git(
        &self,
        attempt: &AttemptId,
        store: &SourceStore,
        m: &CiManifest,
        repo: &ObjectsRepo,
        commit: &str,
        dest: &Path,
    ) -> Result<Workspace, ApiError> {
        if !repo.has_commit(commit) {
            return Err(ApiError::new(
                ErrorCode::NotFound,
                format!("the execution commit {commit} is not in the canonical clone"),
            ));
        }
        let clone = repo.dir();
        let branch = plan::branch_name(attempt);
        // what a create of this attempt that died left is cleared first
        self.reclaim(clone, dest, attempt)?;
        let added = {
            // Held for the whole `fast-worktree add`, not only for the
            // checkout: the project's `[[clone]]` rules copy dependencies out
            // of the clone's main checkout, which must stay at `commit` until
            // that is done.
            let _held = self.hold_clone_at(clone, commit)?;
            self.fw.add(clone, dest, &branch, commit)
        };
        let made = added
            .map_err(|f| plan::fw_error("add", &f))
            .and_then(|()| self.lay_source(store, m, dest))
            .and_then(|()| self.reproduce_head(repo, dest, m, commit))
            .map(|(index_reproduced, warnings)| Workspace {
                cwd: dest.join(m.cwd_relative.to_path()),
                root: dest.to_path_buf(),
                kind: WorkspaceKind::Worktree {
                    clone: clone.to_path_buf(),
                    branch: branch.clone(),
                },
                index_reproduced,
                warnings,
            });
        if made.is_err() {
            // best effort: the original error is the one to report
            let _ = self.teardown_worktree(clone, attempt);
        }
        made
    }

    /// The clone's lock, held, with its main checkout at `commit`. `fast-worktree`
    /// reads the project's configuration from that checkout, so it has to be
    /// at the commit the attempt runs. At the right commit already, adds
    /// share the clone (shared lock); moving it is exclusive, and waits for
    /// the adds in flight.
    fn hold_clone_at(&self, clone: &Path, commit: &str) -> Result<HeldLock, ApiError> {
        let shared = lock_shared(clone)?;
        if self.head_of(clone).as_deref() == Some(commit) {
            return Ok(shared);
        }
        // Releasing the shared lock and taking the exclusive one is not
        // atomic: another writer may move HEAD in between, so what was read
        // above is not trusted and HEAD is read again under the exclusive lock.
        drop(shared);
        let exclusive = lock_exclusive(clone)?;
        if self.head_of(clone).as_deref() != Some(commit) {
            git::run(
                &self.env,
                clone,
                &["-c", NO_REFLOG, "checkout", "-q", "-f", "--detach", commit],
            )
            .map_err(internal)?;
        }
        Ok(exclusive)
    }

    fn head_of(&self, clone: &Path) -> Option<String> {
        git::run(&self.env, clone, &["rev-parse", "HEAD"]).ok()
    }

    /// What a create of `attempt` that died (a crash, a cleaned disk) left: a
    /// directory, the branch, a worktree registration. They are ours by their
    /// names. A directory with none of the others is not a leftover of ours
    /// and is a conflict.
    fn reclaim(&self, clone: &Path, dest: &Path, attempt: &AttemptId) -> Result<(), ApiError> {
        let dir_exists = std::fs::symlink_metadata(dest).is_ok();
        if !dir_exists && !self.branch_exists(clone, attempt) && !self.registered(clone, attempt) {
            return Ok(());
        }
        if dir_exists && !self.branch_exists(clone, attempt) && !self.registered(clone, attempt) {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!("the workspace of {attempt} already exists"),
            ));
        }
        self.teardown_worktree(clone, attempt)
    }

    fn branch_exists(&self, clone: &Path, attempt: &AttemptId) -> bool {
        let r = format!("refs/heads/{}", plan::branch_name(attempt));
        git::ok(&self.env, clone, &["rev-parse", "--verify", "-q", &r])
    }

    fn registered(&self, clone: &Path, attempt: &AttemptId) -> bool {
        let listing =
            git::run(&self.env, clone, &["worktree", "list", "--porcelain"]).unwrap_or_default();
        let mut roots = vec![self.workspaces_root()];
        roots.extend(std::fs::canonicalize(self.workspaces_root()).ok());
        plan::registered_for(&listing, &roots, attempt)
    }

    /// Unpack the source into a scratch directory (verified there, working
    /// directory included), then lay it over the worktree and check the
    /// working directory again.
    fn lay_source(&self, store: &SourceStore, m: &CiManifest, dest: &Path) -> Result<(), ApiError> {
        let staging = OwnedTemp::new_in(&self.workspaces_root(), ".staging.")
            .map_err(|e| io_err("cannot make a scratch directory", e))?;
        store.materialize(m, staging.path()).map_err(internal)?;
        overlay::overlay(staging.path(), dest).map_err(internal)?;
        let cwd = dest.join(m.cwd_relative.to_path());
        match std::fs::symlink_metadata(&cwd) {
            Ok(meta) if meta.is_dir() => Ok(()),
            _ => Err(ApiError::new(
                ErrorCode::Internal,
                format!("the working directory {cwd:?} is not a directory in the workspace"),
            )),
        }
    }

    /// The worktree was made at the execution commit (for a worktree capture,
    /// a synthetic commit on top of the original's HEAD whose tree is the
    /// captured files). Make `git diff` and `git diff --cached` show what the
    /// original's did: the branch goes back to the original's HEAD (the
    /// files stay), and the index becomes the original's `index_tree`. When
    /// the clone has no such tree yet, the index is the original HEAD's tree
    /// instead and the result says so (`false`, with a warning). The bundle
    /// carries `index_tree` (local-ci 0.10), so this is `true` unless the
    /// source predates that or the object is missing.
    fn reproduce_head(
        &self,
        repo: &ObjectsRepo,
        dest: &Path,
        m: &CiManifest,
        commit: &str,
    ) -> Result<(bool, Vec<String>), ApiError> {
        git::run_in_worktree(
            &self.env,
            dest,
            &self.workspaces_root(),
            &["update-ref", CLEANUP_COMMIT, commit],
        )
        .map_err(internal)?;
        let Some(head) = m.original_head.as_deref() else {
            return Ok((
                false,
                vec!["the source has no original HEAD: HEAD and the index are the execution commit's".into()],
            ));
        };
        if head == commit {
            // a commit capture: HEAD and the index are the commit's already
            return Ok((true, Vec::new()));
        }
        for id in [Some(head), m.index_tree.as_deref()].into_iter().flatten() {
            if !plan::is_commit_id(id) {
                return Err(ApiError::new(
                    ErrorCode::InvalidInput,
                    format!("{id:?} in the manifest is not an object id"),
                ));
            }
        }
        if !repo.has_commit(head) {
            return Ok((
                false,
                vec![format!(
                    "the original HEAD {head} is not in the canonical clone: HEAD and the index are the execution commit's"
                )],
            ));
        }
        let ceiling = self.workspaces_root();
        let git = |args: &[&str]| {
            git::run_in_worktree(&self.env, dest, &ceiling, args)
                .map(|_| ())
                .map_err(internal)
        };
        let tree = m.index_tree.as_deref().filter(|t| {
            git::ok(
                &self.env,
                repo.dir(),
                &["cat-file", "-e", &format!("{t}^{{tree}}")],
            )
        });
        match tree {
            Some(t) => {
                git(&["reset", "-q", "--soft", head])?;
                git(&["read-tree", t])?;
                Ok((true, Vec::new()))
            }
            None => {
                git(&["reset", "-q", head])?;
                Ok((
                    false,
                    vec![format!(
                        "the original's index tree is not in the canonical clone: the index is the original HEAD's tree ({head}), so `git diff --cached` shows nothing and `git diff` shows all the changes"
                    )],
                ))
            }
        }
    }

    /// The user's own worktree as the workspace of an `--in-place` run. Nothing
    /// is copied and nothing is removed later; the supervisor keeps
    /// [`plan::IN_PLACE_PROTECTED`] out of the run's reach. The directory has
    /// to be a project directory (the top when it is a Git worktree), not the
    /// home directory or file system root; the resolved cwd stays inside it.
    pub fn open_in_place(
        &self,
        dir: &Path,
        cwd_relative: &Path,
        parent_local: bool,
    ) -> Result<Workspace, ApiError> {
        let real = std::fs::canonicalize(dir)
            .map_err(|e| ApiError::new(ErrorCode::NotFound, format!("{dir:?}: {e}")))?;
        if !real.is_dir() {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                format!("{real:?} is not a directory"),
            ));
        }
        // the state directory may not exist yet: compare against what is there
        let state = std::fs::canonicalize(&self.state_dir).unwrap_or(self.state_dir.clone());
        let home = self
            .env
            .get("HOME")
            .and_then(|h| std::fs::canonicalize(h).ok());
        plan::check_in_place(&real, cwd_relative, &state, home.as_deref(), parent_local)?;
        let top = git::run(&self.env, &real, &["rev-parse", "--show-toplevel"])
            .ok()
            .and_then(|t| std::fs::canonicalize(t).ok());
        if top.is_some() && top.as_deref() != Some(real.as_path()) {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                format!(
                    "--in-place needs the top directory of a Git worktree, {real:?} is not one"
                ),
            ));
        }
        let cwd = std::fs::canonicalize(real.join(cwd_relative)).map_err(|_| {
            ApiError::new(
                ErrorCode::NotFound,
                format!("the working directory {cwd_relative:?} does not exist in {real:?}"),
            )
        })?;
        if !cwd.starts_with(&real) {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                format!("the working directory {cwd_relative:?} leaves {real:?}"),
            ));
        }
        if !cwd.is_dir() {
            return Err(ApiError::new(
                ErrorCode::NotFound,
                format!("the working directory {cwd:?} is not a directory"),
            ));
        }
        Ok(Workspace::plain(real, cwd, WorkspaceKind::InPlace))
    }

    /// Remove a workspace made by [`Workspaces::create`] (idempotent). An
    /// in-place workspace is never removed. The record is checked before it is
    /// used to run anything: the directory has to be an attempt's under the
    /// workspaces root and, for a worktree, the clone and branch the ones
    /// `create` makes.
    ///
    /// fast-worktree reads the removal hooks from the attempt checkout.
    pub fn remove(&self, ws: &Workspace) -> Result<(), ApiError> {
        if ws.kind == WorkspaceKind::InPlace {
            return Ok(());
        }
        let attempt = plan::check_owned(&self.workspaces_root(), &ws.root)?;
        match &ws.kind {
            WorkspaceKind::InPlace => Ok(()),
            WorkspaceKind::Directory => match std::fs::symlink_metadata(&ws.root) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(io_err("cannot inspect the workspace", e)),
                Ok(m) if m.is_dir() => {
                    remove_tree(&ws.root).map_err(|e| io_err("cannot remove the workspace", e))
                }
                Ok(_) => Err(ApiError::new(
                    ErrorCode::Internal,
                    format!("{:?} is not a directory", ws.root),
                )),
            },
            WorkspaceKind::Worktree { clone, branch } => {
                plan::check_worktree_record(&self.clones_root(), clone, branch, &attempt)?;
                self.teardown_worktree(clone, &attempt)
            }
        }
    }

    /// The attempt's own worktree goes whatever the run left in it: tracked
    /// changes and untracked files are reset first (ignored files, the
    /// dependencies, are `fast-worktree`'s to delete), because it refuses a
    /// dirty one. That is done only to a directory that really is a worktree
    /// of this clone (a plain directory, or a `.git` that points elsewhere, is
    /// removed as a plain directory without running Git in it), and Git is
    /// given the worktree explicitly (`GIT_DIR`, `GIT_WORK_TREE`) so it cannot
    /// wander into a repository above. A symlink at the workspace's place is
    /// removed itself, never followed. Then the registration and the branch
    /// are cleared, whether or not the directory was still there.
    fn teardown_worktree(&self, clone: &Path, attempt: &AttemptId) -> Result<(), ApiError> {
        let dir = plan::workspace_dir(&self.workspaces_root(), attempt);
        let mut hook_result = Ok(());
        match std::fs::symlink_metadata(&dir) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("cannot inspect the workspace", e)),
            Ok(m) if m.is_dir() => {
                if self.owned_worktree(clone, &dir) && self.clean_worktree(&dir) {
                    // shared: a prune of the clone's worktree list waits for us
                    let _shared = lock_shared(clone)?;
                    hook_result = self
                        .fw
                        .remove(clone, &dir)
                        .map_err(|f| plan::fw_error("remove", &f));
                } else {
                    remove_tree(&dir).map_err(|e| io_err("cannot remove the workspace", e))?;
                }
            }
            Ok(_) => std::fs::remove_file(&dir)
                .map_err(|e| io_err("cannot remove what is at the workspace's place", e))?,
        }
        // The ports are the worktree's: while it is still there they stay.
        let gone = matches!(
            std::fs::symlink_metadata(&dir),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound
        );
        let released = if gone {
            self.fw
                .release_ports(clone, &dir)
                .map_err(|f| plan::fw_error("release ports", &f))
        } else {
            Ok(())
        };
        hook_result?;
        // A release that cannot be done (a newer registry) must not keep the
        // branch and the registration from being cleared, nor be lost.
        let settled = self.settle(clone, attempt);
        released?;
        settled
    }

    /// Is `dir` a real directory (not a link) whose `.git` is a file that
    /// names a worktree of `clone`?
    fn owned_worktree(&self, clone: &Path, dir: &Path) -> bool {
        use std::io::Read;
        let real_dir = std::fs::symlink_metadata(dir).is_ok_and(|m| m.is_dir());
        let gitfile = dir.join(".git");
        let real_file = std::fs::symlink_metadata(&gitfile).is_ok_and(|m| m.is_file());
        if !real_dir || !real_file {
            return false;
        }
        let mut content = String::new();
        let read =
            std::fs::File::open(&gitfile).and_then(|f| f.take(4096).read_to_string(&mut content));
        if read.is_err() {
            return false;
        }
        let named = plan::gitdir_clone(&content).and_then(|c| std::fs::canonicalize(c).ok());
        named.is_some() && named == std::fs::canonicalize(clone).ok()
    }

    /// Reset and clean the worktree `dir` (already found to be a worktree of
    /// the clone), checking that Git succeeded.
    fn clean_worktree(&self, dir: &Path) -> bool {
        let ceiling = self.workspaces_root();
        let git = |args: &[&str]| git::run_in_worktree(&self.env, dir, &ceiling, args);
        if let Ok(commit) = git(&["rev-parse", "--verify", "--quiet", CLEANUP_COMMIT]) {
            // The running command may have switched branches. Detach before
            // the reset so cleanup cannot move another branch to this commit.
            if git(&["update-ref", "--no-deref", "HEAD", &commit]).is_err() {
                return false;
            }
        }
        // For older workspaces without a saved commit, retain the previous
        // cleanup at HEAD. Ignored dependencies belong to fast-worktree.
        git(&["reset", "-q", "--hard"]).is_ok() && git(&["clean", "-q", "-fd"]).is_ok()
    }

    /// Drop the attempt's branch and registration. A branch whose worktree is
    /// gone cannot be deleted until the registration is pruned, and pruning
    /// also drops the registration of a worktree another attempt is removing
    /// right now, so it is done only when it is needed, under the exclusive
    /// lock (removals hold the shared one).
    fn settle(&self, clone: &Path, attempt: &AttemptId) -> Result<(), ApiError> {
        let branch = plan::branch_name(attempt);
        let clear = !self.registered(clone, attempt)
            && (!self.branch_exists(clone, attempt)
                || git::ok(&self.env, clone, &["branch", "-D", "-q", &branch]));
        if clear {
            return Ok(());
        }
        let _exclusive = lock_exclusive(clone)?;
        let _ = git::run(&self.env, clone, &["worktree", "prune"]);
        if self.branch_exists(clone, attempt) {
            git::run(&self.env, clone, &["branch", "-D", "-q", &branch]).map_err(internal)?;
        }
        Ok(())
    }

    /// Clear what workers that died left under the state directory: scratch
    /// directories (`workspaces/.staging.*`, `clones/.clone.*`) and attempt
    /// workspaces `has_record` does not know. Only for the worker's startup
    /// (S10), under `agent.lock` and before any create runs: a scratch
    /// directory without a marker is removed on that ground alone, and a
    /// create's own would look like a leftover. A leftover worktree is removed like any other
    /// (its clone is read from its `.git` file); anything that cannot be
    /// cleared is reported in `failed` and the sweep goes on.
    pub fn sweep_orphans(
        &self,
        has_record: &dyn Fn(&AttemptId) -> bool,
    ) -> Result<SweepReport, ApiError> {
        let mut report = SweepReport::default();
        let clones = canonical_or(&self.clones_root());
        for (root, scratch_only) in [(self.workspaces_root(), false), (self.clones_root(), true)] {
            // A scratch directory that its maker was still building has a
            // name of its own, which proves who made it (spec 25.4 (a)).
            crate::platform::owned_temp::sweep_unfinished(&root);
            let entries = match std::fs::read_dir(&root) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(io_err("cannot read the state directory", e)),
            };
            for ent in entries.flatten() {
                let path = ent.path();
                let name = ent.file_name().to_string_lossy().into_owned();
                let done = match plan::classify_entry(&name) {
                    plan::Entry::Foreign => continue,
                    plan::Entry::Attempt(_) if scratch_only => continue,
                    plan::Entry::Attempt(a) if has_record(&a) => continue,
                    plan::Entry::Attempt(a) => self.sweep_attempt(&clones, &a),
                    // Without a marker (an older version made it, or its
                    // maker could not say who it was) nothing can be proven
                    // by the directory itself: it goes because this runs at
                    // the worker's start under `agent.lock`, before any
                    // create, where no writer can exist.
                    plan::Entry::Scratch
                        if crate::platform::owned_temp::is_without_marker(&path) =>
                    {
                        remove_any(&path)
                            .map_err(|e| io_err("cannot remove a scratch directory", e))
                    }
                    // A marker proves the owner gone (spec 25.4 (a)); one
                    // that cannot be proven is kept.
                    plan::Entry::Scratch => {
                        if crate::platform::owned_temp::remove_if_owner_gone(&path) {
                            report.removed.push(path);
                        }
                        continue;
                    }
                };
                match done {
                    Ok(()) => report.removed.push(path),
                    Err(e) => report.failed.push((path, e.message)),
                }
            }
        }
        Ok(report)
    }

    fn sweep_attempt(&self, clones: &Path, attempt: &AttemptId) -> Result<(), ApiError> {
        let dir = plan::workspace_dir(&self.workspaces_root(), attempt);
        let named = std::fs::read_to_string(dir.join(".git"))
            .ok()
            .and_then(|c| plan::gitdir_clone(&c))
            .filter(|c| c.parent() == Some(clones));
        match named {
            Some(clone) => self.teardown_worktree(&clone, attempt),
            None => remove_any(&dir).map_err(|e| io_err("cannot remove a leftover workspace", e)),
        }
    }
}

/// What [`Workspaces::sweep_orphans`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub removed: Vec<PathBuf>,
    /// Entries that could not be cleared, with why.
    pub failed: Vec<(PathBuf, String)>,
}

fn canonical_or(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

/// The same place: written the same, or both existing paths that resolve to
/// one (a clone seeded by a URL is compared by the string).
fn same_location(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    let (pa, pb) = (Path::new(a), Path::new(b));
    pa.is_absolute()
        && pb.is_absolute()
        && matches!(
            (std::fs::canonicalize(pa), std::fs::canonicalize(pb)),
            (Ok(x), Ok(y)) if x == y
        )
}

/// A directory tree removed even where the run left directories it cannot
/// read (a mode of 000): their permissions are given back first. Links are
/// not followed.
fn remove_tree(path: &Path) -> std::io::Result<()> {
    fn open_up(p: &Path) {
        if !std::fs::symlink_metadata(p).is_ok_and(|m| m.is_dir()) {
            return;
        }
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700));
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                open_up(&e.path());
            }
        }
    }
    open_up(path);
    std::fs::remove_dir_all(path)
}

/// A directory tree, or a file or link itself.
fn remove_any(path: &Path) -> std::io::Result<()> {
    if std::fs::symlink_metadata(path)?.is_dir() {
        remove_tree(path)
    } else {
        std::fs::remove_file(path)
    }
}

fn open_lock(path: &Path) -> Result<File, ApiError> {
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .map_err(|e| io_err("cannot open a lock file", e))
}

/// The clone's main lock, shared. A writer that is waiting for it holds the
/// intent lock, so this queues behind the writer instead of joining the
/// readers that keep it from ever getting in.
fn lock_shared(clone: &Path) -> Result<HeldLock, ApiError> {
    lock_main(clone, File::lock_shared)
}

/// The clone's main lock, exclusive (writer priority: see [`lock_shared`]).
fn lock_exclusive(clone: &Path) -> Result<HeldLock, ApiError> {
    lock_main(clone, File::lock)
}

fn lock_main(clone: &Path, take: fn(&File) -> std::io::Result<()>) -> Result<HeldLock, ApiError> {
    let git = clone.join(".git");
    let intent = open_lock(&git.join("belt-workspace.intent"))?;
    let main = open_lock(&git.join("belt-workspace.lock"))?;
    let err = |e| io_err("cannot lock the clone", e);
    intent.lock().map_err(err)?;
    let _intent = HeldLock::new(intent);
    // the intent lock is released when `_intent` is dropped, on any return
    take(&main).map_err(err)?;
    Ok(HeldLock::new(main))
}

/// A clone's directory name: `r-<16 lowercase hex>`.
fn clone_name_ok(clone: &str) -> bool {
    clone.len() == 18
        && clone.starts_with("r-")
        && clone[2..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
