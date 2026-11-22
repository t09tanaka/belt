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
const FILL_LOCK: &str = "fill.lock";
pub const FILL_LOG: &str = "fill.log";
/// `fill.log` is started afresh once it grows past this.
const LOG_LIMIT: u64 = 1 << 20;
/// `misses` keeps the `add`s that found no spare for this long (it is
/// trimmed whenever one is added to), and `fill.log` stays this long after
/// the last fill wrote to it (and at most [`LOG_LIMIT`] long): a pool size
/// is judged by the last week, and the log is read to see why a fill failed.
const KEEP_SECS: u64 = 7 * 86_400;
/// A pool directory is removed once nothing is in it but its lock, and it
/// has not been changed for this long: a fill that has just made it is
/// about to put something in it.
const IDLE_SECS: u64 = 60;
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
/// [`KEEP_SECS`] while at it.
pub fn record_miss(main: &Path) {
    let pool = dir(main);
    let _ = fs::create_dir_all(&pool);
    let path = pool.join(MISSES);
    let now = now_secs();
    if let Some(trimmed) = fs::read_to_string(&path)
        .ok()
        .and_then(|t| kept_misses(&t, now))
    {
        let _ = fs::write(&path, trimmed);
    }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = writeln!(f, "{now}");
    }
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
    crate::retention::run(Some(&main));
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
    sweep_locked(Some(&main), &pool);
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
    while spares(pool).len() < size as usize {
        if let Err(e) = make_spare(main, pool, base) {
            outln!("{} fill  failed: {e}", stamp());
            return Ok(1);
        }
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

/// Which of the hidden entries in a pool directory, `names`, a killed run
/// left. `fill_idle` says that no fill is running (the fill lock is held by
/// the caller): only then is a `.new-*` or `.no-hooks-*` anybody's
/// leftover, since only a fill makes them. A `.claimed-<pid>-*` is a spare
/// that the `add` with that pid took, and is left while that process is
/// alive (`alive`). A name that is not one of these is left alone.
fn stale_leftovers(names: &[String], fill_idle: bool, alive: &dyn Fn(u32) -> bool) -> Vec<String> {
    names
        .iter()
        .filter(|name| {
            if let Some(id) = name
                .strip_prefix(NO_HOOKS_PREFIX)
                .or_else(|| name.strip_prefix(BUILDING_PREFIX))
            {
                return fill_idle && created_of(id).is_some();
            }
            if let Some((pid, id)) = name
                .strip_prefix(CLAIMED_PREFIX)
                .and_then(|r| r.split_once('-'))
            {
                return created_of(id).is_some() && pid.parse::<u32>().is_ok_and(|p| !alive(p));
            }
            false
        })
        .cloned()
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

/// Remove, from `pool`: the `misses` older than [`KEEP_SECS`] (the file
/// when none is left), and, unless a fill is running, `fill.log` when it has
/// not been written for that long and the leftovers of killed runs
/// ([`stale_leftovers`]). `main` is the repository's main checkout when it
/// is known; a leftover is unregistered from it, else from the one its own
/// `.git` file names. Nothing a live process may use is touched, and
/// nothing is reported.
pub fn sweep_in(main: Option<&Path>, pool: &Path) {
    let misses = pool.join(MISSES);
    if let Ok(text) = fs::read_to_string(&misses) {
        match kept_misses(&text, now_secs()) {
            Some(t) if t.is_empty() => {
                let _ = fs::remove_file(&misses);
            }
            Some(t) => {
                let _ = fs::write(&misses, t);
            }
            None => {}
        }
    }
    let Ok(Some(_lock)) = try_lock(pool) else {
        return;
    };
    sweep_locked(main, pool);
}

/// [`sweep_in`] for the part that needs the fill lock, which the caller
/// holds.
fn sweep_locked(main: Option<&Path>, pool: &Path) {
    let log = pool.join(FILL_LOG);
    let idle = fs::metadata(&log)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|m| SystemTime::now().duration_since(m).ok())
        .is_some_and(|age| age.as_secs() > KEEP_SECS);
    if idle {
        let _ = fs::remove_file(&log);
    }
    let names: Vec<String> = fs::read_dir(pool)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    for name in stale_leftovers(&names, true, &cache::pid_alive) {
        let path = pool.join(&name);
        if name.starts_with(NO_HOOKS_PREFIX) {
            // Empty, unless something put a file there.
            let _ = fs::remove_dir(&path);
            continue;
        }
        let main = main
            .map(Path::to_path_buf)
            .or_else(|| install::main_checkout(&path).ok());
        match main {
            Some(m) => discard(&m, &path),
            None => {
                let _ = trash::trash(&path);
            }
        }
    }
}

/// [`sweep_in`] for a pool whose repository is not known, then remove the
/// pool directory when nothing is left in it but its fill lock, no fill is
/// running, and it had not been changed for [`IDLE_SECS`] before the sweep
/// (a fill that has just made it is about to put something in it). The lock
/// file goes first; a fill that waits on it takes it again on a new file
/// ([`util::lock_path`]).
pub fn tidy(pool: &Path) {
    let young = fs::metadata(pool)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|m| SystemTime::now().duration_since(m).ok())
        .is_none_or(|age| age.as_secs() < IDLE_SECS);
    sweep_in(None, pool);
    if young {
        return;
    }
    let Ok(Some(_lock)) = try_lock(pool) else {
        return;
    };
    let only_lock = fs::read_dir(pool).is_ok_and(|d| {
        d.flatten()
            .all(|e| e.file_name().to_string_lossy() == FILL_LOCK)
    });
    if only_lock && fs::remove_file(pool.join(FILL_LOCK)).is_ok() {
        let _ = fs::remove_dir(pool);
    }
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
    sweep_in(Some(&main), &pool);
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
        let all = names(&[
            ".new-wt-100-1-0",
            ".no-hooks-wt-100-1-0",
            ".claimed-77-wt-100-1-0",
            "wt-100-1-0",
            ".new-wt-x",
            ".claimed-x-wt-5-1-0",
            "fill.lock",
        ]);
        assert_eq!(
            stale_leftovers(&all, true, &dead),
            names(&[
                ".new-wt-100-1-0",
                ".no-hooks-wt-100-1-0",
                ".claimed-77-wt-100-1-0"
            ]),
            "a ready spare and unknown names stay"
        );
        // A fill is running: what it makes is not a leftover.
        assert_eq!(
            stale_leftovers(&all, false, &dead),
            names(&[".claimed-77-wt-100-1-0"])
        );
        // The process that claimed a spare is alive.
        assert_eq!(
            stale_leftovers(&all, true, &alive),
            names(&[".new-wt-100-1-0", ".no-hooks-wt-100-1-0"])
        );
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
