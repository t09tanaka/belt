//! The pool: worktrees made ahead of time, with their dependencies
//! installed, that `add` takes instead of creating one.
//!
//! Spares live in `<cache>/<repo>/pool/wt-<unix seconds>-<pid>-<n>`. Each is
//! `git worktree add --detach` of origin's default branch, then `install`,
//! then `git worktree lock --reason "fast-worktree pool"`, built under a
//! hidden name (`.new-<id>`) and renamed into place only when ready. The
//! repository's own git hooks do not run for a spare (`core.hooksPath` is
//! an empty directory for that `git worktree add`): whatever a
//! `post-checkout` hook sets up belongs to the worktree where it ends up,
//! and it runs there, with the `git switch` that puts the spare on its
//! branch.
//!
//! Taking one is a rename to a hidden name of our own (`.claimed-<pid>-<id>`):
//! the rename either succeeds or finds the spare gone, so two `add`s never
//! take the same one. Then `git worktree repair` (git still records the old
//! path), `unlock`, and `git worktree move` to where the worktree belongs.
//! If the move fails (another file system, say), the spare is put back.
//!
//! `fill` holds `pool/fill.lock` (`flock`) while it works, so only one runs
//! per repository; a second one sees the lock and exits. The refill that an
//! `add` (or `hook create`) starts, whether it took a spare or found none,
//! starts when that command is done, and waits for its background install
//! when there is one (`pool fill --after <worktree>`), so that the two do
//! not install at the same time.
//!
//! The pool is off unless `[pool] size` is set, in the main checkout's
//! `.fast-worktree.toml` or in the user's `config.toml`.
//!
//! A spare is made in the pool and used somewhere else, so whatever its
//! install recorded by absolute path points into the pool. A `node_modules`
//! with a symlink whose target is absolute is therefore not kept in a spare
//! (the `add` that takes it installs that project again); after the move,
//! the path where the spare was made is replaced with the new one in the
//! files Flutter generated ([`crate::ecosystem::dart::rewrite_moved_paths`]),
//! and the npm projects' own install steps run again where the worktree now
//! is ([`crate::ecosystem::Preset::rerun_own_scripts`]).

use crate::cmd::install;
use crate::ecosystem::{self, target, Mode};
use crate::error::{bail, Error, Result};
use crate::hooks::{self, Config};
use crate::util::{errln, out, outln};
use crate::{cache, clone, git, paths, pending, trash, util};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const LOCK_REASON: &str = "fast-worktree pool";
/// Spares are named `wt-<unix seconds>-<pid>-<n>`.
pub const SPARE_PREFIX: &str = "wt-";
const BUILDING_PREFIX: &str = ".new-";
const CLAIMED_PREFIX: &str = ".claimed-";
/// The empty directory a spare's `git worktree add` takes for its hooks.
const NO_HOOKS_PREFIX: &str = ".no-hooks-";
/// How long a refill waits for the background install it was started
/// after.
const AFTER_LIMIT: Duration = Duration::from_secs(30 * 60);
const MISSES: &str = "misses";
pub(crate) const FILL_LOCK: &str = "fill.lock";
pub const FILL_LOG: &str = "fill.log";
/// `fill.log` is started afresh once it grows past this.
const LOG_LIMIT: u64 = 1 << 20;
/// `misses` keeps the `add`s that found no spare for this long (it is
/// trimmed whenever one is added to), and `fill.log` stays this long after
/// the last fill wrote to it (and at most [`LOG_LIMIT`] long): a pool size
/// is judged by the last week, and the log is read to see why a fill failed.
const KEEP_SECS: u64 = 7 * 86_400;
/// A leftover whose owner is alive by its pid is still removed once it is
/// this old: the pid may be another process's by now. Making a spare, or
/// moving one to where it belongs, takes minutes at most.
const LEFTOVER_MAX_SECS: u64 = 86_400;
/// A leftover whose owner is gone by its pid still stays until its inode
/// last changed this long ago, as a half-made cache entry does
/// ([`cache::TMP_MIN_AGE`]): a pid says nothing about a process in another
/// pid namespace (a container that shares the cache), where the fill that
/// is making it may be alive.
const LEFTOVER_MIN_SECS: u64 = cache::TMP_MIN_AGE.as_secs();
/// How long `pool fill` lets the fetch of the default branch take.
const FETCH_LIMIT: Duration = Duration::from_secs(60);

/// The pool directory of the repository whose main checkout is `main`.
pub fn dir(main: &Path) -> PathBuf {
    paths::cache_root().join(paths::repo_key(main)).join("pool")
}

/// Whether `path` (a real path) is a spare, or one being made or taken.
pub fn is_pool_path(path: &Path) -> bool {
    let root = paths::cache_root();
    let root = root.canonicalize().unwrap_or(root);
    path.starts_with(&root)
        && path
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|n| n == "pool")
}

/// Where the size comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Source {
    /// `[pool] size` in the main checkout's `.fast-worktree.toml`.
    Repo,
    /// `[pool] size` in the user-wide settings file.
    User,
    /// Not set anywhere: the pool is off.
    Default,
}

impl Source {
    fn describe(self) -> String {
        match self {
            Source::Repo => format!("[pool] in the main checkout's {}", hooks::FILE),
            Source::User => format!("[pool] in {}", util::tilde(&hooks::user_config_path())),
            Source::Default => "not set".to_string(),
        }
    }
}

/// The configured size: `[pool] size` in the main checkout's
/// `.fast-worktree.toml`, else in the user's config, else 0 (off). A file
/// that cannot be read is reported and counts as unset.
pub fn size(main: &Path) -> (u32, Source) {
    let repo = Config::load(main)
        .map_err(|e| errln!("fast-worktree: {e}"))
        .ok()
        .and_then(|c| c.pool)
        .and_then(|p| p.size);
    if let Some(n) = repo {
        return (n, Source::Repo);
    }
    match hooks::user_pool_size() {
        Ok(Some(n)) => (n, Source::User),
        Ok(None) => (0, Source::Default),
        Err(e) => {
            errln!("fast-worktree: {e}");
            (0, Source::Default)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spare {
    pub path: PathBuf,
    pub id: String,
    /// Unix seconds, from the id.
    pub created: u64,
}

fn created_of(id: &str) -> Option<u64> {
    id.strip_prefix(SPARE_PREFIX)?
        .split('-')
        .next()?
        .parse()
        .ok()
}

/// The ready spares, newest first.
pub fn spares(pool: &Path) -> Vec<Spare> {
    let mut v: Vec<Spare> = fs::read_dir(pool)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter_map(|e| {
            let id = e.file_name().to_string_lossy().into_owned();
            let created = created_of(&id)?;
            Some(Spare {
                path: e.path(),
                id,
                created,
            })
        })
        .collect();
    v.sort_by(|a, b| (b.created, &b.id).cmp(&(a.created, &a.id)));
    v
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn new_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "{SPARE_PREFIX}{}-{}-{}",
        now_secs(),
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// What [`take`] got.
#[derive(Debug)]
pub enum Taken {
    /// The spare is now the worktree at the target; `made_at` are the
    /// paths of the directory it was made in (as written, and the real
    /// path).
    Spare { made_at: Vec<PathBuf> },
    /// No spare was ready.
    Empty,
    /// A spare was found but could not be moved; it was put back.
    Failed(String),
}

/// Take a spare and move it to `target` (which must not exist). The
/// worktree there is detached at the spare's commit.
pub fn take(main: &Path, target: &Path) -> Taken {
    let pool = dir(main);
    for s in spares(&pool) {
        let claimed = pool.join(format!("{CLAIMED_PREFIX}{}-{}", std::process::id(), s.id));
        if fs::rename(&s.path, &claimed).is_err() {
            continue; // another `add` took it first
        }
        // When it was taken, for the sweep (the rename itself already says
        // so in the inode change time, where the file system keeps one).
        cache::touch(&claimed);
        return match settle(main, &claimed, target) {
            Ok(()) => Taken::Spare {
                made_at: building_paths(&pool, &s.id),
            },
            Err(why) => {
                put_back(main, &claimed, &s.path);
                Taken::Failed(why.to_string())
            }
        };
    }
    Taken::Empty
}

/// Tell git where the claimed spare is, unlock it and move it to `target`.
fn settle(main: &Path, claimed: &Path, target: &Path) -> Result<()> {
    let claimed_s = claimed.to_string_lossy();
    git::output(main, &["worktree", "repair", &claimed_s])?;
    git::output(claimed, &["rev-parse", "HEAD"])?;
    git::output(main, &["worktree", "unlock", &claimed_s])?;
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Err(e) = git::output(
        main,
        &["worktree", "move", &claimed_s, &target.to_string_lossy()],
    ) {
        let _ = git::output(
            main,
            &["worktree", "lock", "--reason", LOCK_REASON, &claimed_s],
        );
        return Err(e);
    }
    Ok(())
}

/// Undo a claim: lock again and rename back.
fn put_back(main: &Path, claimed: &Path, spare: &Path) {
    let claimed_s = claimed.to_string_lossy();
    let _ = git::output(
        main,
        &["worktree", "lock", "--reason", LOCK_REASON, &claimed_s],
    );
    if fs::rename(claimed, spare).is_ok() {
        let _ = git::output(main, &["worktree", "repair", &spare.to_string_lossy()]);
    }
}

/// Where the spare `id` was made: `<pool>/.new-<id>`, as `pool` is written
/// and as its real path, when that differs.
fn building_paths(pool: &Path, id: &str) -> Vec<PathBuf> {
    let name = format!("{BUILDING_PREFIX}{id}");
    let mut v = vec![pool.join(&name)];
    if let Ok(real) = pool.canonicalize() {
        let real = real.join(&name);
        if !v.contains(&real) {
            v.push(real);
        }
    }
    v
}

/// Note that an `add` found no spare, and drop the notes older than
/// [`KEEP_SECS`] while at it. The file is read and written holding a lock
/// on the file itself, so that `add`s at the same time each leave their
/// line.
pub fn record_miss(main: &Path) {
    let path = dir(main).join(MISSES);
    let now = now_secs();
    let _ = edit_misses(&path, true, |text| {
        let mut kept = kept_misses(text, now).unwrap_or_else(|| text.to_string());
        kept.push_str(&format!("{now}\n"));
        Some(kept)
    });
}

/// Rewrite the `misses` file at `path` under an exclusive lock on it:
/// `edit` gets its text and returns the new one (`None`: leave it). A file
/// that would be empty is removed, which is safe because whoever waited for
/// the lock looks at the path again ([`util::lock_path`]; no version locks
/// this path without doing so). With `make` false a file that is not there
/// is not made.
fn edit_misses(path: &Path, make: bool, edit: impl Fn(&str) -> Option<String>) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    if !make && !path.exists() {
        return Ok(());
    }
    let Some(mut f) = util::lock_path(path, true, make)? else {
        return Ok(());
    };
    let mut text = String::new();
    // Opened for writing only: read through a second handle.
    File::open(path)?.read_to_string(&mut text)?;
    let Some(new) = edit(&text) else {
        return if text.is_empty() {
            fs::remove_file(path)
        } else {
            Ok(())
        };
    };
    if new.is_empty() {
        return fs::remove_file(path);
    }
    f.set_len(0)?;
    f.seek(SeekFrom::Start(0))?;
    f.write_all(new.as_bytes())
}

/// How many times an `add` found no spare.
pub fn misses(pool: &Path) -> usize {
    fs::read_to_string(pool.join(MISSES))
        .map(|s| s.lines().filter(|l| !l.is_empty()).count())
        .unwrap_or(0)
}

/// Start `fast-worktree pool fill` in the background, detached, with its
/// output in `pool/fill.log`. With `after`, it first waits for the
/// background install of that worktree, if one is running.
pub fn spawn_fill(main: &Path, after: Option<&Path>) {
    let pool = dir(main);
    if let Err(e) = try_spawn_fill(main, &pool, after) {
        outln!("note   pool: cannot start `pool fill`: {e}");
    }
}

fn try_spawn_fill(main: &Path, pool: &Path, after: Option<&Path>) -> io::Result<()> {
    fs::create_dir_all(pool)?;
    let log_path = pool.join(FILL_LOG);
    let fresh = fs::metadata(&log_path).is_ok_and(|m| m.len() > LOG_LIMIT);
    let log = OpenOptions::new()
        .create(true)
        .append(!fresh)
        .write(true)
        .truncate(fresh)
        .open(&log_path)?;
    let exe = util::helper_exe()?;
    let mut c = Command::new(exe);
    c.args(["pool", "fill"]);
    if let Some(a) = after {
        c.arg("--after").arg(a);
    }
    c.current_dir(main)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    pending::detach(&mut c);
    for v in ["GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_PREFIX"] {
        c.env_remove(v);
    }
    c.spawn().map(util::release)
}

/// An exclusive `flock` on `pool/fill.lock`, held while the value lives.
pub struct FillLock(#[allow(dead_code)] File);

/// Take the fill lock without waiting. `None` when another process has it.
pub fn try_lock(pool: &Path) -> Result<Option<FillLock>> {
    match util::lock_path(&pool.join(FILL_LOCK), false, true) {
        Ok(f) => Ok(f.map(FillLock)),
        Err(e) => Err(Error::msg(format!("{FILL_LOCK}: {e}"))),
    }
}

/// The main checkout of the repository in the current directory.
fn main_here() -> Result<PathBuf> {
    install::main_checkout(&git::toplevel(Path::new("."))?)
}

fn stamp() -> String {
    cache::datetime(SystemTime::now())
}

/// Fetch origin's default branch and return `origin/<branch>`. A failed
/// fetch is reported and the ref as last fetched is used.
fn fetched_base(main: &Path) -> Result<String> {
    let base = git::origin_default_branch(main)?;
    if let Err(e) = git::fetch_quietly(main, &base, Some(FETCH_LIMIT)) {
        outln!("note   pool: fetch failed, using origin/{base} as last fetched: {e}");
    }
    Ok(format!("origin/{base}"))
}

/// `pool fill`: make spares until there are `size`. With `after`, first
/// wait (holding the fill lock) for the background install of that
/// worktree, if one is running.
pub fn fill(after: Option<&Path>) -> Result<i32> {
    fill_for(&main_here()?, after)
}

/// [`fill`] for the repository whose main checkout is `main`.
pub fn fill_for(main: &Path, after: Option<&Path>) -> Result<i32> {
    let main = main.to_path_buf();
    let (size, _) = size(&main);
    if size == 0 {
        outln!("the pool is off: set [pool] size in {}", hooks::FILE);
        return Ok(0);
    }
    // Not the part that deletes kept refs: this mostly runs in the
    // background, where nobody reads what it says.
    crate::retention::run_in_fill(&main);
    let pool = dir(&main);
    let Some(_lock) = try_lock(&pool)? else {
        outln!("{} fill  another fill is running", stamp());
        return Ok(0);
    };
    if let Some(a) = after {
        wait_for_install(a);
    }
    outln!("{} fill  started", stamp());
    // No other fill is running: what an earlier one left half made is not
    // in use, and is removed before this one adds to the pool.
    sweep_leftovers(&main, &pool, true);
    let base = fetched_base(&main)?;
    fill_locked(&main, &pool, size, &base)
}

/// Wait while the background install of the worktree `root` runs, for at
/// most [`AFTER_LIMIT`].
fn wait_for_install(root: &Path) {
    if pending::state(root) != pending::State::Running {
        return;
    }
    outln!(
        "{} fill  waiting for the background install in {}",
        stamp(),
        util::tilde(root)
    );
    let start = Instant::now();
    while pending::state(root) == pending::State::Running {
        if start.elapsed() > AFTER_LIMIT {
            outln!(
                "{} fill  still running after {}, filling anyway",
                stamp(),
                util::elapsed(start)
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn fill_locked(main: &Path, pool: &Path, size: u32, base: &str) -> Result<i32> {
    let mut size = size;
    while spares(pool).len() < size as usize {
        if let Err(e) = make_spare(main, pool, base) {
            outln!("{} fill  failed: {e}", stamp());
            return Ok(1);
        }
        // A size that was lowered meanwhile holds from now on: the spares
        // beyond it would be discarded by the next command ([`trim`]).
        size = size_if_readable(main).unwrap_or(size);
    }
    outln!(
        "{} fill  done: {} of {size} ready",
        stamp(),
        spares(pool).len()
    );
    Ok(0)
}

/// Make one spare at `base`.
fn make_spare(main: &Path, pool: &Path, base: &str) -> Result<()> {
    let start = Instant::now();
    let id = new_id();
    let building = pool.join(format!("{BUILDING_PREFIX}{id}"));
    let b = building.to_string_lossy().into_owned();
    // None of the repository's hooks: what a post-checkout hook sets up
    // (and a post-checkout hook running fast-worktree would install) is for
    // the worktree where the spare ends up, where it runs on `git switch`.
    let no_hooks = pool.join(format!("{NO_HOOKS_PREFIX}{id}"));
    fs::create_dir_all(&no_hooks)?;
    let hooks_path = format!("core.hooksPath={}", no_hooks.display());
    let added = git::output_with_env(
        main,
        &[
            "-c",
            &hooks_path,
            "worktree",
            "add",
            "-q",
            "--detach",
            &b,
            base,
        ],
        &[("FAST_WORKTREE_SKIP", "1")],
    );
    let _ = fs::remove_dir(&no_hooks);
    added?;
    let installed = Config::load(&building)
        .and_then(|cfg| install::install_deps(&building, &[], &cfg, Mode::Missing))
        .map(|i| i.ok);
    if !matches!(installed, Ok(true)) {
        discard(main, &building);
        match installed {
            Err(e) => bail!("installing into a new spare: {e}"),
            _ => bail!("installing into a new spare failed"),
        }
    }
    if let Ok(cfg) = Config::load(&building) {
        drop_absolute_symlinks(&building, &cfg);
    }
    git::output(main, &["worktree", "lock", "--reason", LOCK_REASON, &b])?;
    let ready = pool.join(&id);
    fs::rename(&building, &ready)?;
    git::output(main, &["worktree", "repair", &ready.to_string_lossy()])?;
    let short = git::output(&ready, &["rev-parse", "--short", "HEAD"])?;
    outln!(
        "{} spare {id} at {} ready ({})",
        stamp(),
        short.trim(),
        util::elapsed(start)
    );
    Ok(())
}

/// Move away the `node_modules` of the spare at `root` that have a symlink
/// with an absolute target: it points into the pool, where the spare will
/// not be. The `add` that takes the spare installs those projects again.
fn drop_absolute_symlinks(root: &Path, cfg: &Config) {
    for p in target::targets(root, cfg).unwrap_or_default() {
        if p.preset.name() != "npm" {
            continue;
        }
        let nm = ecosystem::project_path(root, &p.dir).join("node_modules");
        if nm.is_dir() && clone::has_absolute_symlink(&nm) {
            let _ = trash::trash(&nm);
            outln!(
                "{} note  {}: absolute symlink in node_modules, not kept in the spare",
                stamp(),
                p.dir
            );
        }
    }
}

/// Remove a spare (or what is left of one) and unregister it, and only it.
fn discard(main: &Path, path: &Path) {
    // It may have been renamed since git last saw it: tell git, so that
    // its entry can be found from where it is now.
    let _ = git::output(main, &["worktree", "repair", &path.to_string_lossy()]);
    let admin = git::admin_dir(path)
        .ok()
        .or_else(|| git::find_admin(main, path).ok().flatten());
    if trash::trash(path).is_err() {
        return;
    }
    match admin.map(|a| git::unregister(&a)) {
        Some(Ok(())) => {}
        Some(Err(e)) => outln!("note   pool: {e}"),
        // A spare whose `git worktree add` never finished has no entry.
        None => {}
    }
}

/// Claim a spare and discard it. False when another process took it first.
fn claim_and_discard(main: &Path, pool: &Path, s: &Spare) -> bool {
    let claimed = pool.join(format!("{CLAIMED_PREFIX}{}-{}", std::process::id(), s.id));
    if fs::rename(&s.path, &claimed).is_err() {
        return false;
    }
    discard(main, &claimed);
    true
}

/// How many commits `commit` is behind `base`, if git can tell.
fn behind(main: &Path, commit: &str, base: &str) -> Option<u64> {
    git::output(main, &["rev-list", "--count", &format!("{commit}..{base}")])
        .ok()?
        .trim()
        .parse()
        .ok()
}

fn head_of(spare: &Path) -> Option<String> {
    git::output(spare, &["rev-parse", "HEAD"])
        .ok()
        .map(|s| s.trim().to_string())
}

/// `pool status`.
pub fn status() -> Result<i32> {
    let main = main_here()?;
    let (size, source) = size(&main);
    let pool = dir(&main);
    let ready = spares(&pool);
    if size == 0 {
        outln!("pool   off (size {})", source.describe());
    } else {
        outln!(
            "pool   {} of {size} ready (size from {})",
            ready.len(),
            source.describe()
        );
    }
    if !ready.is_empty() {
        let base = git::origin_default_branch(&main)
            .map(|b| format!("origin/{b}"))
            .ok();
        let rows: Vec<[String; 4]> = ready
            .iter()
            .map(|s| {
                let head = head_of(&s.path);
                let short = head
                    .as_deref()
                    .map(|h| h[..h.len().min(7)].to_string())
                    .unwrap_or_else(|| "?".into());
                let behind = match (&head, &base) {
                    (Some(h), Some(b)) => behind(&main, h, b)
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "?".into()),
                    _ => "?".into(),
                };
                let created = cache::datetime(UNIX_EPOCH + Duration::from_secs(s.created)) + " UTC";
                [s.id.clone(), short, behind, created]
            })
            .collect();
        let behind_head = match &base {
            Some(b) => format!("BEHIND {b}"),
            None => "BEHIND".to_string(),
        };
        out!(
            "{}",
            crate::cmd::cache::table(&["ID", "COMMIT", &behind_head, "CREATED"], &rows)
        );
    }
    let fill_running = matches!(try_lock(&pool), Ok(None));
    outln!("misses {} (an add found no spare ready)", misses(&pool));
    if fill_running {
        outln!(
            "fill   running (log: {})",
            util::tilde(&pool.join(FILL_LOG))
        );
    }
    Ok(0)
}

/// `pool refresh`: fetch, discard the spares behind origin's default
/// branch, and fill again.
pub fn refresh() -> Result<i32> {
    let main = main_here()?;
    let (size, _) = size(&main);
    let pool = dir(&main);
    let Some(_lock) = try_lock(&pool)? else {
        bail!("a fill is running; try again when it is done");
    };
    let base = fetched_base(&main)?;
    let mut discarded = 0;
    for s in spares(&pool) {
        let stale = match head_of(&s.path) {
            Some(h) => behind(&main, &h, &base) != Some(0),
            None => true,
        };
        if stale && claim_and_discard(&main, &pool, &s) {
            outln!("drop   {} (behind {base})", s.id);
            discarded += 1;
        }
    }
    outln!("dropped {discarded} spare(s) behind {base}");
    if size == 0 {
        outln!("the pool is off: set [pool] size in {}", hooks::FILE);
        return Ok(0);
    }
    fill_locked(&main, &pool, size, &base)
}

/// The pid in a spare's id, `wt-<unix seconds>-<pid>-<n>`: the process
/// that made it.
fn maker_of(id: &str) -> Option<u32> {
    let mut parts = id.strip_prefix(SPARE_PREFIX)?.split('-');
    parts.next()?.parse::<u64>().ok()?;
    let pid = parts.next()?.parse().ok()?;
    parts.next()?.parse::<u64>().ok()?;
    parts.next().is_none().then_some(pid)
}

/// Which of the hidden entries in a pool directory a killed run left.
/// `entries` are their names with how many seconds ago each one's inode
/// last changed; `now` is in unix seconds. A name that is not one of these
/// is left alone.
///
/// A `.new-<id>` or `.no-hooks-<id>` is what the fill with the pid in the
/// id is making: a leftover when that process is gone (`alive`) or the
/// caller holds the fill lock (`fill_idle`: only a fill makes them, and no
/// fill is running), and its inode last changed at least `min_age` seconds
/// ago; or, whatever the pid says, when the time in the id is more than
/// [`LEFTOVER_MAX_SECS`] ago. So the sweep of a command needs no fill lock
/// to tell, and a fill that starts meanwhile is not turned away.
///
/// A `.claimed-<pid>-<id>` is a spare that the `add` with that pid took: a
/// leftover when that process is gone and it was taken (its inode last
/// changed) at least `min_age` seconds ago, or when it was taken more than
/// [`LEFTOVER_MAX_SECS`] ago.
///
/// `min_age` ([`LEFTOVER_MIN_SECS`]) is there because neither a pid nor,
/// on some shared file systems, a lock is seen from another machine or
/// container that uses the same cache.
fn stale_leftovers(
    entries: &[(String, u64)],
    now: u64,
    fill_idle: bool,
    min_age: u64,
    alive: &dyn Fn(u32) -> bool,
) -> Vec<String> {
    entries
        .iter()
        .filter(|(name, changed_ago)| {
            let settled = *changed_ago >= min_age;
            if let Some(id) = name
                .strip_prefix(NO_HOOKS_PREFIX)
                .or_else(|| name.strip_prefix(BUILDING_PREFIX))
            {
                let (Some(created), Some(pid)) = (created_of(id), maker_of(id)) else {
                    return false;
                };
                return ((fill_idle || !alive(pid)) && settled)
                    || created + LEFTOVER_MAX_SECS < now;
            }
            if let Some((pid, id)) = name
                .strip_prefix(CLAIMED_PREFIX)
                .and_then(|r| r.split_once('-'))
            {
                return maker_of(id).is_some()
                    && pid
                        .parse::<u32>()
                        .is_ok_and(|p| (!alive(p) && settled) || *changed_ago > LEFTOVER_MAX_SECS);
            }
            false
        })
        .map(|(name, _)| name.clone())
        .collect()
}

/// The lines of `misses` that stay at time `now`: those from the last
/// [`KEEP_SECS`]. `None` when there is nothing to drop.
fn kept_misses(text: &str, now: u64) -> Option<String> {
    let keep = |l: &str| l.trim().parse::<u64>().is_ok_and(|t| t + KEEP_SECS >= now);
    if text.lines().all(keep) {
        return None;
    }
    let mut out = String::new();
    for l in text.lines().filter(|l| keep(l)) {
        out.push_str(l.trim());
        out.push('\n');
    }
    Some(out)
}

/// Remove, from the pool `pool` of the repository at `main`: the `misses`
/// older than [`KEEP_SECS`] (the file when none is left), `fill.log` when
/// it has not been written for that long, and the leftovers of killed runs
/// ([`stale_leftovers`]). Nothing a live process may use is touched, and
/// nothing is reported.
///
/// The fill lock is not taken: a fill that starts while this runs must not
/// find it held and give up. The pool directory and its `fill.lock` stay
/// while the repository is there: every version takes that lock on
/// whatever file it opened, so the path is never unlinked.
pub fn sweep_in(main: &Path, pool: &Path) {
    let now = now_secs();
    let _ = edit_misses(&pool.join(MISSES), false, |text| kept_misses(text, now));
    let log = pool.join(FILL_LOG);
    let idle = fs::metadata(&log)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|m| SystemTime::now().duration_since(m).ok())
        .is_some_and(|age| age.as_secs() > KEEP_SECS);
    if idle {
        let _ = fs::remove_file(&log);
    }
    sweep_leftovers(main, pool, false);
}

/// Discard the leftovers of killed runs in `pool` ([`stale_leftovers`]).
/// `fill_idle`: the caller holds the fill lock.
fn sweep_leftovers(main: &Path, pool: &Path, fill_idle: bool) {
    sweep_leftovers_aged(main, pool, fill_idle, LEFTOVER_MIN_SECS);
}

/// [`sweep_leftovers`] with the least age of a leftover given.
fn sweep_leftovers_aged(main: &Path, pool: &Path, fill_idle: bool, min_age: u64) {
    let wall = SystemTime::now();
    let entries: Vec<(String, u64)> = fs::read_dir(pool)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            if !name.starts_with('.') {
                return None;
            }
            let changed = e
                .metadata()
                .map_or(0, |m| cache::changed_ago(&m, wall).as_secs());
            Some((name, changed))
        })
        .collect();
    for name in stale_leftovers(&entries, now_secs(), fill_idle, min_age, &cache::pid_alive) {
        let path = pool.join(&name);
        if name.starts_with(NO_HOOKS_PREFIX) {
            // Empty, unless something put a file there.
            let _ = fs::remove_dir(&path);
        } else {
            discard(main, &path);
        }
    }
}

/// Whether `name` in a pool directory is a spare in any state: ready
/// (`wt-<id>`), being made (`.new-<id>`, `.no-hooks-<id>`) or taken
/// (`.claimed-<pid>-<id>`). Its id, when it is.
fn spare_id(name: &str) -> Option<&str> {
    let id = if let Some(rest) = name.strip_prefix(CLAIMED_PREFIX) {
        let (pid, id) = rest.split_once('-')?;
        pid.parse::<u32>().ok()?;
        id
    } else {
        name.strip_prefix(BUILDING_PREFIX)
            .or_else(|| name.strip_prefix(NO_HOOKS_PREFIX))
            .unwrap_or(name)
    };
    maker_of(id).map(|_| id)
}

/// Empty the pool directory `pool` of a repository that ran no command for
/// [`crate::retention::IDLE`]: a spare is there for the next `add` of its
/// repository, and nothing shows that one is coming. The spares in every
/// state go, and `fill.log` and `misses` (which only say something about
/// the last 7 days). `fill.lock` and the directory stay
/// ([`crate::retention`]); so does every name that is not one of these.
///
/// With the main checkout `main` (it is there), each spare is discarded as
/// everywhere else: unregistered, then deleted. Without it (the checkout
/// cannot be reached: a volume that is not mounted, a checkout that was
/// moved) the directories are only moved into the trash, added to `moved`
/// for the caller's deleter, and their registrations stay in the
/// repository until its next command ([`drop_dangling`]).
///
/// Only the detached process of the walk calls this. The fill lock is
/// taken without waiting. Returns false when a fill holds it: nothing was
/// done, and the next walk tries again.
pub fn retire(pool: &Path, main: Option<&Path>, moved: &mut Vec<PathBuf>) -> bool {
    let wall = SystemTime::now();
    let entries: Vec<(String, u64)> = fs::read_dir(pool)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let age = e
                .metadata()
                .ok()
                .map(|m| cache::changed_ago(&m, wall).as_secs());
            // Unknown age cannot prove a leftover safe to discard.
            if age.is_none() && name.starts_with('.') {
                return None;
            }
            Some((name, age.unwrap_or(0)))
        })
        .collect();
    let names = retirement_names(&entries, now_secs(), &cache::pid_alive);
    if names.is_empty() {
        return true;
    }
    let _lock = match util::lock_path(&pool.join(FILL_LOCK), false, false) {
        Ok(Some(f)) => Some(f),
        Ok(None) => return false,
        // A file system without locks: no fill holds it either.
        Err(_) => None,
    };
    let now = now_secs();
    for name in names {
        let path = pool.join(&name);
        if name == FILL_LOG {
            let _ = fs::remove_file(&path);
        } else if name == MISSES {
            let _ = edit_misses(&path, false, |text| kept_misses(text, now));
        } else if name.starts_with(NO_HOOKS_PREFIX) {
            let _ = fs::remove_dir(&path);
        } else if let Some(main) = main {
            // Under a name of our own first, as `add` takes one: an `add`
            // of an older version (which does not say that the repository
            // is in use) then finds no spare, rather than half of one.
            let claimed = match name.starts_with(SPARE_PREFIX) {
                true => pool.join(format!("{CLAIMED_PREFIX}{}-{name}", std::process::id())),
                false => path.clone(),
            };
            if claimed == path || fs::rename(&path, &claimed).is_ok() {
                discard(main, &claimed);
            }
        } else if let Ok(dest) = trash::move_aside(&path) {
            moved.push(dest);
        }
    }
    true
}

fn retirement_names(
    entries: &[(String, u64)],
    now: u64,
    alive: &dyn Fn(u32) -> bool,
) -> Vec<String> {
    // Keep live owners even with the fill lock: another host may share this pool.
    let stale = stale_leftovers(entries, now, false, LEFTOVER_MIN_SECS, alive);
    entries
        .iter()
        .filter(|(name, _)| {
            name == FILL_LOG
                || name == MISSES
                || (name.starts_with(SPARE_PREFIX) && spare_id(name).is_some())
                || stale.contains(name)
        })
        .map(|(name, _)| name.clone())
        .collect()
}

/// The pool directory and the id of the spare that a registration in
/// `.git/worktrees` is for: `locked` is what its `locked` file holds, and
/// `worktree` the path its `gitdir` file gives. `None` unless it is locked
/// with exactly the reason every version locks its spares with
/// ([`LOCK_REASON`]; a worktree that the user or another tool locked is
/// never ours to touch), and unless the path is
/// `<cache>/<repository>/pool/<a spare's name>` in this cache (`roots`:
/// the cache root as it is written and as it resolves). A spare that
/// another machine made in a cache of its own, for a repository on a
/// shared disk, is then left alone unless that cache is at the same path.
fn registered_spare<'a>(
    locked: Option<&str>,
    worktree: &'a Path,
    roots: &[PathBuf],
) -> Option<(&'a Path, &'a str)> {
    let reason = locked?;
    if reason.strip_suffix('\n').unwrap_or(reason) != LOCK_REASON {
        return None;
    }
    let id = spare_id(worktree.file_name()?.to_str()?)?;
    let pool = worktree.parent()?;
    let repo = pool.parent()?;
    let ours = pool.file_name()? == "pool"
        && cache::is_repo_name(repo.file_name()?.to_str()?)
        && roots.iter().any(|r| repo.parent() == Some(r.as_path()));
    ours.then_some((pool, id))
}

/// Unregister the spares of the repository at `main` whose directories
/// are no longer there: the registrations (`.git/worktrees/wt-*`, each
/// with an index) that are left when the cache directory of the repository
/// went without git being told. That happens when the repository was not
/// in use and could not be reached ([`retire`]), and when its checkout was
/// moved: the spares stay behind under the old path's name and go with it
/// ([`crate::retention`]), while the registrations moved with `.git`.
/// They are locked, so `git worktree prune` keeps them for ever.
///
/// Only a registration that [`registered_spare`] says is a spare's, whose
/// directory does not exist ("no such file", looked for inside the cache
/// only), and whose id no entry of its pool directory has (a spare is
/// renamed while it is made and while it is taken, and git is told just
/// after). All of it is looked at again holding the registration lock,
/// which every `git worktree` command of fast-worktree holds; the lock is
/// not waited for. Returns how many were unregistered.
pub fn drop_dangling(main: &Path) -> usize {
    let root = paths::cache_root();
    let mut roots = vec![root.clone()];
    if let Ok(real) = root.canonicalize() {
        if real != root {
            roots.push(real);
        }
    }
    let dangling = |admin: &Path| -> bool {
        let locked = fs::read_to_string(admin.join("locked")).ok();
        let Ok(gitdir) = fs::read_to_string(admin.join("gitdir")) else {
            return false;
        };
        // `<worktree>/.git`, absolute or relative to the entry.
        let dot_git = admin.join(gitdir.trim_end_matches('\n'));
        let Some(worktree) = dot_git.parent() else {
            return false;
        };
        let Some((pool, id)) = registered_spare(locked.as_deref(), worktree, &roots) else {
            return false;
        };
        let missing = |e: &io::Error| e.kind() == io::ErrorKind::NotFound;
        if !fs::symlink_metadata(worktree).is_err_and(|e| missing(&e)) {
            return false;
        }
        match fs::read_dir(pool) {
            Ok(entries) => !entries
                .flatten()
                .any(|e| e.file_name().to_string_lossy().ends_with(id)),
            Err(e) => missing(&e),
        }
    };
    git::unregister_where(&main.join(".git"), &dangling)
}

/// [`size`] when the settings can be read, without reporting when they
/// cannot: for the sweeps, which must not take a broken settings file for
/// "the pool is off".
fn size_if_readable(main: &Path) -> Option<u32> {
    let repo = Config::load(main).ok()?.pool.and_then(|p| p.size);
    match repo {
        Some(n) => Some(n),
        None => Some(hooks::user_pool_size().ok()?.unwrap_or(0)),
    }
}

/// Which of `ready` (the ready spares, newest first) are beyond the pool
/// size `size`: all of them when the pool is off, else the oldest.
fn surplus(ready: &[Spare], size: u32) -> &[Spare] {
    &ready[ready.len().min(size as usize)..]
}

/// Discard the spares of the repository at `main` that are beyond its
/// configured pool size: all of them when the pool was turned off. A spare
/// is there only while its repository is in use ([`retire`]), the pool is
/// on, and it is within the size; every command in the repository checks (one directory
/// listing, and nothing more while the pool is empty). Returns how many
/// were discarded.
pub fn trim(main: &Path) -> usize {
    let pool = dir(main);
    let ready = spares(&pool);
    if ready.is_empty() {
        return 0;
    }
    let Some(size) = size_if_readable(main) else {
        return 0;
    };
    surplus(&ready, size)
        .iter()
        .filter(|s| claim_and_discard(main, &pool, s))
        .count()
}

/// `pool clear`: remove every spare, and what killed runs left behind.
pub fn clear() -> Result<i32> {
    let main = main_here()?;
    let pool = dir(&main);
    let mut n = 0;
    for s in spares(&pool) {
        if claim_and_discard(&main, &pool, &s) {
            n += 1;
        }
    }
    sweep_in(&main, &pool);
    outln!("removed {n} spare(s) (deleting in background)");
    if matches!(try_lock(&pool), Ok(None)) {
        outln!("note   a fill is running and may add spares again");
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spares_are_ready_dirs_newest_first() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path();
        for d in [
            "wt-100-1-0",
            "wt-300-1-0",
            "wt-200-9-3",
            ".new-wt-400-1-0",
            ".claimed-5-wt-500-1-0",
            "wt-x",
        ] {
            fs::create_dir(p.join(d)).unwrap();
        }
        fs::write(p.join("wt-600-1-0"), "a file").unwrap();
        fs::write(p.join(MISSES), "1\n2\n").unwrap();
        let ids: Vec<String> = spares(p).into_iter().map(|s| s.id).collect();
        assert_eq!(ids, ["wt-300-1-0", "wt-200-9-3", "wt-100-1-0"]);
        assert_eq!(spares(p)[0].created, 300);
        assert_eq!(misses(p), 2);
        assert_eq!(misses(&p.join("none")), 0);
    }

    #[test]
    fn ids_are_unique_and_dated() {
        let a = new_id();
        let b = new_id();
        assert_ne!(a, b);
        assert!(created_of(&a).is_some_and(|t| t + 5 > now_secs()));
    }

    #[test]
    fn the_fill_lock_is_exclusive() {
        let t = tempfile::tempdir().unwrap();
        let held = try_lock(t.path()).unwrap();
        assert!(held.is_some());
        // flock is per open file: a second open in this process conflicts.
        assert!(try_lock(t.path()).unwrap().is_none());
        drop(held);
        // A child another test is forking right now may hold a copy of the
        // descriptor until it execs.
        let start = Instant::now();
        while try_lock(t.path()).unwrap().is_none() {
            assert!(start.elapsed() < Duration::from_secs(5), "still locked");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn leftovers_are_those_nothing_alive_can_own() {
        let dead = |_: u32| false;
        let alive = |_: u32| true;
        let now = 1_000_000;
        let all: Vec<(String, u64)> = names(&[
            ".new-wt-999900-1-0",
            ".no-hooks-wt-999900-1-0",
            ".claimed-77-wt-999900-1-0",
            "wt-999900-1-0",
            ".new-wt-x",
            ".new-wt-999900-x-0",
            ".claimed-x-wt-5-1-0",
            "fill.lock",
        ])
        .into_iter()
        .map(|n| (n, 10))
        .collect();
        let ours = names(&[
            ".new-wt-999900-1-0",
            ".no-hooks-wt-999900-1-0",
            ".claimed-77-wt-999900-1-0",
        ]);
        assert_eq!(
            stale_leftovers(&all, now, false, 10, &dead),
            ours,
            "a ready spare and unknown names stay"
        );
        // Their owners are alive: the fill that makes one, the `add` that
        // took one. Nothing is a leftover, and no fill lock was needed to
        // tell.
        assert!(stale_leftovers(&all, now, false, 10, &alive).is_empty());
        // The caller holds the fill lock: no fill is running, whatever the
        // pids say.
        assert_eq!(stale_leftovers(&all, now, true, 10, &alive), ours[..2]);
        // A second short of the least age, nothing is a leftover yet:
        // neither a pid nor a lock is seen from another container.
        assert!(stale_leftovers(&all, now, false, 11, &dead).is_empty());
        assert!(stale_leftovers(&all, now, true, 11, &dead).is_empty());
        assert_eq!(LEFTOVER_MIN_SECS, 600);
    }

    #[test]
    fn retirement_keeps_live_and_young_leftovers() {
        let now = 1_000_000;
        let leftovers = names(&[
            ".new-wt-999900-1-0",
            ".no-hooks-wt-999900-1-0",
            ".claimed-77-wt-999900-1-0",
        ]);
        let entries = |age| {
            leftovers
                .iter()
                .cloned()
                .chain(names(&["wt-999900-1-0", "fill.lock", ".new-wt-x"]))
                .map(|name| (name, age))
                .collect::<Vec<_>>()
        };
        let plain = names(&["wt-999900-1-0"]);
        assert_eq!(
            retirement_names(&entries(LEFTOVER_MIN_SECS), now, &|_| true),
            plain
        );
        assert_eq!(
            retirement_names(&entries(LEFTOVER_MIN_SECS - 1), now, &|_| false),
            plain
        );
        let mut discarded = leftovers.clone();
        discarded.extend(plain);
        assert_eq!(
            retirement_names(&entries(LEFTOVER_MIN_SECS), now, &|_| false),
            discarded
        );
    }

    #[test]
    fn a_reused_pid_cannot_keep_a_leftover_for_ever() {
        let alive = |_: u32| true;
        let now = 1_800_000_000;
        let at = |created: u64, changed_ago: u64| {
            let e = vec![
                (format!(".new-wt-{created}-1-0"), changed_ago),
                (format!(".claimed-77-wt-{created}-1-0"), changed_ago),
            ];
            stale_leftovers(&e, now, false, LEFTOVER_MIN_SECS, &alive)
        };
        let limit = LEFTOVER_MAX_SECS;
        // Exactly at the limit both stay; a second past it they go. What
        // is being made is judged by the time in its id, what was taken by
        // when it was taken (a spare may be days old when it is taken).
        assert!(at(now - limit, limit).is_empty());
        assert_eq!(at(now - limit - 1, 0), vec![".new-wt-1799913599-1-0"]);
        assert_eq!(at(now, limit + 1), vec![".claimed-77-wt-1800000000-1-0"]);
        assert!(at(now - 30 * 86_400, 0)
            .iter()
            .all(|n| n.starts_with(".new-")));
    }

    #[test]
    fn a_leftover_made_just_now_stays_and_an_old_one_is_discarded() {
        let t = tempfile::tempdir().unwrap();
        std::env::set_var("FAST_WORKTREE_CACHE", t.path().join("cache"));
        let pool = t.path().join("cache/repo-0123abcd/pool");
        let made = now_secs() - 7_200;
        let building = pool.join(format!(".new-wt-{made}-2000000000-0"));
        let claimed = pool.join(format!(".claimed-2000000000-wt-{made}-1-1"));
        for d in [&building, &claimed] {
            fs::create_dir_all(d.join("sub")).unwrap();
        }
        // Their makers are gone, but they were put there this second: a
        // fill in another container may be making them.
        sweep_leftovers(t.path(), &pool, false);
        sweep_leftovers(t.path(), &pool, true);
        assert!(building.is_dir() && claimed.is_dir());
        // With no least age (the inode change time cannot be set back).
        sweep_leftovers_aged(t.path(), &pool, false, 0);
        assert!(!building.exists() && !claimed.exists());
    }

    #[test]
    fn a_spare_is_known_by_its_name_in_every_state() {
        assert_eq!(spare_id("wt-1700000000-12-0"), Some("wt-1700000000-12-0"));
        assert_eq!(
            spare_id(".new-wt-1700000000-12-0"),
            Some("wt-1700000000-12-0")
        );
        assert_eq!(
            spare_id(".no-hooks-wt-1700000000-12-0"),
            Some("wt-1700000000-12-0")
        );
        assert_eq!(
            spare_id(".claimed-7-wt-1700000000-12-0"),
            Some("wt-1700000000-12-0")
        );
        for other in [
            "fill.lock",
            "misses",
            "wt-x",
            ".claimed-x-wt-1-1-0",
            ".something-else",
            "wt-1-1",
        ] {
            assert_eq!(spare_id(other), None, "{other}");
        }
    }

    #[test]
    fn only_a_registration_locked_with_our_reason_in_this_cache_is_a_spare_s() {
        let roots = vec![PathBuf::from("/c/fast-worktree")];
        let spare = Path::new("/c/fast-worktree/app-0123abcd/pool/wt-1700000000-12-0");
        let ours = Some((spare.parent().unwrap(), "wt-1700000000-12-0"));
        let reason = format!("{LOCK_REASON}\n");
        assert_eq!(registered_spare(Some(&reason), spare, &roots), ours);
        assert_eq!(registered_spare(Some(LOCK_REASON), spare, &roots), ours);
        // While it is made or taken, the same spare.
        let taken = spare.with_file_name(".claimed-9-wt-1700000000-12-0");
        assert_eq!(
            registered_spare(Some(&reason), &taken, &roots).map(|s| s.1),
            Some("wt-1700000000-12-0")
        );
        // Not locked, or locked by the user or another tool: never ours.
        for other in [
            None,
            Some(""),
            Some("fast-worktree pool 2\n"),
            Some("mine\n"),
            Some("fast-worktree pool\n\n"),
            Some(" fast-worktree pool\n"),
        ] {
            assert_eq!(registered_spare(other, spare, &roots), None, "{other:?}");
        }
        // Locked with our reason, but not at a spare's place in this cache.
        for path in [
            "/c/other-cache/app-0123abcd/pool/wt-1700000000-12-0",
            "/c/fast-worktree/app-0123abcd/wt-1700000000-12-0",
            "/c/fast-worktree/app/pool/wt-1700000000-12-0",
            "/c/fast-worktree/x/app-0123abcd/pool/wt-1700000000-12-0",
            "/c/fast-worktree/app-0123abcd/pool/feature",
            "/work/app-wt",
        ] {
            assert_eq!(
                registered_spare(Some(&reason), Path::new(path), &roots),
                None,
                "{path}"
            );
        }
    }

    #[test]
    fn taking_a_spare_shows_in_its_inode_change_time() {
        // What the age of a `.claimed-*` is read from: a rename (and the
        // touch after it) sets the time, however old the directory.
        let t = tempfile::tempdir().unwrap();
        let spare = t.path().join("wt-1-1-0");
        fs::create_dir(&spare).unwrap();
        std::thread::sleep(Duration::from_millis(2_100));
        let age = |p: &Path| cache::changed_ago(&fs::metadata(p).unwrap(), SystemTime::now());
        assert!(age(&spare) >= Duration::from_secs(2));
        let claimed = t.path().join(".claimed-1-wt-1-1-0");
        fs::rename(&spare, &claimed).unwrap();
        cache::touch(&claimed);
        assert!(age(&claimed) < Duration::from_secs(2));
    }

    #[test]
    fn spares_beyond_the_size_are_surplus_oldest_first() {
        let spare = |created: u64| Spare {
            path: PathBuf::from(format!("/p/wt-{created}-1-0")),
            id: format!("wt-{created}-1-0"),
            created,
        };
        let ready = [spare(300), spare(200), spare(100)];
        assert!(surplus(&ready, 3).is_empty() && surplus(&ready, 9).is_empty());
        assert_eq!(surplus(&ready, 2), &ready[2..]);
        assert_eq!(surplus(&ready, 0), &ready[..], "the pool is off");
    }

    #[test]
    fn misses_written_at_the_same_time_are_all_kept() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("pool/misses");
        let now = now_secs();
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for _ in 0..25 {
                        edit_misses(&path, true, |text| Some(format!("{text}{now}\n"))).unwrap();
                    }
                });
            }
        });
        assert_eq!(misses(t.path().join("pool").as_path()), 200);
        // Trimmed to nothing, the file goes; one that is not there is not
        // made by a sweep.
        edit_misses(&path, false, |_| Some(String::new())).unwrap();
        assert!(!path.exists());
        edit_misses(&path, false, |text| kept_misses(text, now)).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn misses_older_than_the_limit_are_dropped() {
        let now = 10_000_000;
        let edge = now - KEEP_SECS;
        assert_eq!(kept_misses(&format!("{edge}\n{now}\n"), now), None);
        assert_eq!(
            kept_misses(&format!("{}\n{edge}\n", edge - 1), now),
            Some(format!("{edge}\n"))
        );
        assert_eq!(
            kept_misses(&format!("{}\n", edge - 1), now),
            Some(String::new())
        );
        assert_eq!(kept_misses("junk\n", now), Some(String::new()));
        assert_eq!(kept_misses("", now), None);
    }
}
