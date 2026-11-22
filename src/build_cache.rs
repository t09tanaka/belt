//! A cache of the `target/` directory of Rust jobs, shared by the runs of a machine.
//!
//! Every job runs in a worktree of its own, so every job would build all its
//! dependencies again. An entry of this cache is the `target/` that a job left
//! when it passed. The next job with the same key gets a copy of it (a
//! copy-on-write clone where the file system has them) and cargo finds
//! everything Fresh: the workspace crates are fingerprinted by their path
//! relative to the workspace, and the dependencies by their path in the
//! registry, and neither changes from one worktree to the next.
//!
//! An entry holds only what cargo owns: for each profile directory (`debug/`,
//! `release/`, `<triple>/<profile>/`) its `deps`, `build` and `.fingerprint`
//! (and `.cargo-lock`), and the two files at the root of `target/`. Whatever
//! else a job left there (`tmp`, nextest, `llvm-cov-target`, profile data,
//! `criterion`, `incremental`, the output of examples) is not kept.
//!
//! Entries are immutable: `<dir>/<key>/target`, written under a temporary name
//! and renamed into place, so that two jobs saving the same key cannot mix
//! theirs. A writer holds an exclusive lock on `<tmp>.lock` for the whole
//! save: a temporary entry whose lock can be taken belongs to a writer that is
//! gone. The marker file in an entry holds its size, and its mtime is the time
//! of its last use (for the least-recently-used pruning).
//!
//! The key holds the toolchain as `rustc -vV` and `cargo -V` report it before
//! the job's steps run. A toolchain that a setup step of the job installs is
//! not seen by the key: the job uses the one that is there at the start.
//!
//! Nothing here may fail a job: the functions report, and the caller warns.
//! Entries are deleted only by moving them into the trash directory; when the
//! move fails the entry stays and a warning says so.

use crate::context::ExecContext;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime};

/// Changes when what an entry holds, or what its key covers, changes.
pub const SCHEMA: u32 = 2;

/// The file in an entry that holds its size in bytes.
const MARKER: &str = ".local-ci-entry";

/// Directory name (of the cache directory) the entries live in.
pub const DIR: &str = "build";

/// Total size of the cache before the least recently used entries go, in GiB.
const DEFAULT_MAX_GB: f64 = 20.0;

/// The most a copy that is not a clone may take, in MiB. Above it the cache
/// is skipped rather than slow the job down with a copy.
const DEFAULT_COPY_LIMIT_MB: u64 = 512;

/// A temporary entry this old, without a lock file, was left by a job that
/// died (one with a lock file is judged by the lock).
const STALE_TMP: Duration = Duration::from_secs(3600);

/// The most the `rustc -vV` and `cargo -V` of the key may take together.
const DEFAULT_PROBE_SECS: u64 = 20;

/// Where the cache is and how much it may hold.
#[derive(Debug, Clone)]
pub struct Settings {
    /// `<cache dir>/build`
    pub dir: PathBuf,
    /// Where pruned entries go before they are deleted in the background.
    pub trash: PathBuf,
    pub max_bytes: u64,
    /// The size above which an entry is not copied when cloning is not
    /// certain.
    pub copy_limit: u64,
    /// The most the toolchain probe of a job may take.
    pub probe_timeout: Duration,
}

impl Settings {
    /// The settings for a cache directory, from the environment of `ctx`:
    /// `LOCAL_CI_NO_BUILD_CACHE` (anything but empty or `0`) turns the cache
    /// off (`None`), `LOCAL_CI_BUILD_CACHE_MAX_GB` sets the size cap (default
    /// 20; `0` turns the cache off) and `LOCAL_CI_BUILD_CACHE_COPY_LIMIT_MB` the
    /// most that is copied where cloning is not available (default 512).
    /// `LOCAL_CI_BUILD_CACHE_PROBE_TIMEOUT_SECS` bounds the toolchain probe
    /// (default 20).
    pub fn from_ctx(ctx: &ExecContext, cache_dir: &Path, disabled: bool) -> Option<Settings> {
        if disabled || ctx.var("LOCAL_CI_NO_BUILD_CACHE").is_some_and(truthy) {
            return None;
        }
        Some(Self::at(ctx, cache_dir)).filter(|s| s.max_bytes > 0)
    }

    /// The settings whether or not the cache is turned off: for looking at
    /// the entries and removing them.
    pub fn at(ctx: &ExecContext, cache_dir: &Path) -> Settings {
        let gb = ctx
            .var("LOCAL_CI_BUILD_CACHE_MAX_GB")
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|g| g.is_finite() && *g >= 0.0)
            .unwrap_or(DEFAULT_MAX_GB);
        let mb = ctx
            .var("LOCAL_CI_BUILD_CACHE_COPY_LIMIT_MB")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_COPY_LIMIT_MB);
        let probe = ctx
            .var("LOCAL_CI_BUILD_CACHE_PROBE_TIMEOUT_SECS")
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|s| s.is_finite() && *s > 0.0)
            .map(Duration::from_secs_f64)
            .unwrap_or(Duration::from_secs(DEFAULT_PROBE_SECS));
        Settings {
            dir: cache_dir.join(DIR),
            trash: cache_dir.join("trash"),
            max_bytes: (gb * 1024.0 * 1024.0 * 1024.0) as u64,
            copy_limit: mb * 1024 * 1024,
            probe_timeout: probe,
        }
    }
}

pub(crate) fn truthy(v: &str) -> bool {
    !matches!(v.trim(), "" | "0" | "false" | "no" | "off")
}

/// What a job did with the cache, as it comes to `result.json`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct JobCache {
    /// The first 12 characters of the key.
    pub key: String,
    /// `target/` was filled from an entry.
    #[serde(default)]
    pub restored: bool,
    /// The job added an entry.
    #[serde(default)]
    pub saved: bool,
}

/// `build cache  restored 2 · saved 1` for the summary; empty when no job
/// restored or saved anything.
pub fn summary<'a>(jobs: impl Iterator<Item = &'a JobCache>) -> String {
    let (mut r, mut s) = (0, 0);
    for j in jobs {
        r += usize::from(j.restored);
        s += usize::from(j.saved);
    }
    if r + s == 0 {
        return String::new();
    }
    format!("build cache  restored {r} · saved {s}")
}

// ---------------------------------------------------------------- key

/// What the key is made of.
#[derive(Debug, Clone, Default)]
pub struct KeyInputs {
    /// `rustc -vV`
    pub rustc: String,
    /// `cargo -V`
    pub cargo: String,
    pub os_arch: String,
    /// the `Cargo.lock` of the checkout's root: relative path and the hash of its content
    pub locks: Vec<(String, String)>,
    /// the workflow file and the job id
    pub job: String,
    /// the variables that change a build (as the job evaluates them), see [`build_env`]
    pub env: Vec<(String, String)>,
    /// `.cargo/config.toml`, `.cargo/config`, `rust-toolchain.toml` and
    /// `rust-toolchain` of the checkout: name and the hash of the content
    pub files: Vec<(String, String)>,
    /// the values of the job's matrix entry
    pub matrix: String,
}

pub fn key(i: &KeyInputs) -> String {
    let mut h = Sha256::new();
    let mut part = |label: &str, text: &str| {
        // a length in front of each part, so that no two inputs run together
        h.update(format!("{label}:{}:", text.len()).as_bytes());
        h.update(text.as_bytes());
    };
    part("schema", &SCHEMA.to_string());
    part("rustc", &i.rustc);
    part("cargo", &i.cargo);
    part("os", &i.os_arch);
    part("job", &i.job);
    for (p, d) in &i.locks {
        part("lock", &format!("{p}={d}"));
    }
    for (k, v) in &i.env {
        part("env", &format!("{k}={v}"));
    }
    for (n, d) in &i.files {
        part("file", &format!("{n}={d}"));
    }
    part("matrix", &i.matrix);
    hex(&h.finalize())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Whether a variable changes what cargo builds (and so which `target/` is
/// right): `RUSTFLAGS`, `RUSTDOCFLAGS`, `CARGO_*` except the ones that only
/// set how many threads a job uses or how output looks, `RUSTC_WRAPPER`, the
/// coverage and LLVM variables.
pub fn affects_build(name: &str) -> bool {
    if crate::cpu::JOB_THREAD_ENV.contains(&name) {
        return false;
    }
    if name.starts_with("CARGO_TERM_") {
        return false;
    }
    matches!(
        name,
        "RUSTFLAGS"
            | "RUSTDOCFLAGS"
            | "RUSTC_WRAPPER"
            | "RUSTC_WORKSPACE_WRAPPER"
            | "RUSTUP_TOOLCHAIN"
            | "LLVM_PROFILE_FILE"
            | "LLVM_COV"
            | "LLVM_PROFDATA"
    ) || name.starts_with("CARGO_")
        || name.starts_with("LLVM_COV_")
}

/// The variables out of `vars` that [`affects_build`], sorted, with the path
/// of the workspace in a value replaced (it differs from one worktree to the
/// next and would make every key new).
pub fn build_env<'a>(
    vars: impl IntoIterator<Item = (&'a str, &'a str)>,
    ws: &Path,
) -> Vec<(String, String)> {
    let ws = ws.to_string_lossy();
    let mut m: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in vars {
        if affects_build(k) {
            let v = if ws.is_empty() {
                v.to_string()
            } else {
                v.replace(ws.as_ref(), "<workspace>")
            };
            m.insert(k.to_string(), v);
        }
    }
    m.into_iter().collect()
}

/// A job that puts its build elsewhere has no `target/` to cache.
pub fn moves_target(env: &[(String, String)]) -> bool {
    env.iter()
        .any(|(k, _)| k == "CARGO_TARGET_DIR" || k == "CARGO_BUILD_TARGET_DIR")
}

/// A checkout that holds a Rust workspace: `Cargo.toml` and `Cargo.lock` at its root.
pub fn applies(ws: &Path) -> bool {
    ws.join("Cargo.toml").is_file() && ws.join("Cargo.lock").is_file()
}

/// The `Cargo.lock` at the root of `ws` (the cache is of the root crate
/// only), as relative path and the hash of its content; empty when there is none.
pub fn lock_digests(ws: &Path) -> Vec<(String, String)> {
    std::fs::read(ws.join("Cargo.lock"))
        .map(|b| vec![("Cargo.lock".to_string(), hex(&Sha256::digest(&b)))])
        .unwrap_or_default()
}

/// The files of the checkout that choose the toolchain and how cargo builds,
/// by name, with the hash of their content (those that exist).
pub fn config_digests(ws: &Path) -> Vec<(String, String)> {
    [
        ".cargo/config.toml",
        ".cargo/config",
        "rust-toolchain.toml",
        "rust-toolchain",
    ]
    .into_iter()
    .filter_map(|n| {
        let p = ws.join(n);
        std::fs::symlink_metadata(&p).ok()?;
        let d = std::fs::read(&p)
            .map(|b| hex(&Sha256::digest(&b)))
            .unwrap_or_else(|e| format!("unreadable:{}", e.kind()));
        Some((n.to_string(), d))
    })
    .collect()
}

/// The `rustc -vV` and `cargo -V` of the checkout (so that `rust-toolchain.toml`
/// is honoured), run with `path` as `PATH`, together within `timeout`. A
/// toolchain that is not installed is not installed by this
/// (`RUSTUP_AUTO_INSTALL=0`). `Err` with the reason when either is missing,
/// fails or is too slow: the job then goes without the cache.
pub fn toolchain(
    ctx: &ExecContext,
    ws: &Path,
    path: &str,
    timeout: Duration,
) -> Result<(String, String), String> {
    let deadline = Instant::now() + timeout;
    let run = |prog: &str, arg: &str| -> Result<String, String> {
        use std::io::Read;
        use std::os::unix::process::CommandExt;
        let mut child = ctx
            .command(prog)
            .arg(arg)
            .env("PATH", path)
            .env("RUSTUP_AUTO_INSTALL", "0")
            .current_dir(ws)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .map_err(|e| format!("`{prog} {arg}`: {e}"))?;
        let pg = child.id() as i32;
        let kill = || {
            // SAFETY: a signal to the group this function started.
            unsafe {
                libc::killpg(pg, libc::SIGKILL);
            }
        };
        let status = loop {
            match child.try_wait() {
                Ok(Some(st)) => break st,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Ok(None) => {
                    kill();
                    let _ = child.wait();
                    return Err(format!(
                        "`{prog} {arg}` took more than {:.0}s",
                        timeout.as_secs_f64()
                    ));
                }
                Err(e) => {
                    kill();
                    return Err(format!("`{prog} {arg}`: {e}"));
                }
            }
        };
        // nothing of the group may hold the pipe open while it is read
        kill();
        let mut bytes = Vec::new();
        if let Some(mut o) = child.stdout.take() {
            let _ = o.read_to_end(&mut bytes);
        }
        let text = String::from_utf8_lossy(&bytes).trim().to_string();
        if status.success() && !text.is_empty() {
            Ok(text)
        } else {
            Err(format!("`{prog} {arg}` failed ({status})"))
        }
    };
    Ok((run("rustc", "-vV")?, run("cargo", "-V")?))
}

pub fn os_arch() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

// ---------------------------------------------------------- allowlist

/// Directories at the root of `target/` that hold a build of their own and
/// are not a target triple.
fn not_a_triple(dir: &Path, name: &str) -> bool {
    // a target directory of its own (llvm-cov, a tool's) has the files cargo
    // writes at the root of one; a target triple's directory has not
    dir.join("CACHEDIR.TAG").exists()
        || dir.join(".rustc_info.json").exists()
        || matches!(
            name,
            "llvm-cov-target" | "nextest" | "tmp" | "criterion" | "doc" | "package"
        )
        || name.starts_with("mutants.out")
}

/// The lock files cargo keeps in a profile directory (which of them, depends on its version).
const LOCKS: [&str; 3] = [".cargo-lock", ".cargo-build-lock", ".cargo-artifact-lock"];

/// A directory cargo builds in: it has a `.fingerprint` directory (the layout
/// of older cargo) or one of its lock files (the newer layout keeps
/// everything under `build/`).
fn is_profile_dir(p: &Path) -> bool {
    crate::git::is_real_dir(&p.join(".fingerprint"))
        || LOCKS
            .iter()
            .any(|l| std::fs::symlink_metadata(p.join(l)).is_ok_and(|m| m.is_file()))
}

/// What an entry holds, as paths relative to `target`: the files cargo
/// keeps at its root, and for each profile directory (`debug`, `release`,
/// `<triple>/debug`, ...) its `deps`, `build` and `.fingerprint` and the
/// lock files. A directory is a profile directory when it has a
/// `.fingerprint` directory or a lock file of cargo's. Everything else in `target/` is left out.
pub fn allowed(target: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let profile = |rel: PathBuf, out: &mut Vec<PathBuf>| {
        for d in ["deps", "build", ".fingerprint"] {
            if crate::git::is_real_dir(&target.join(&rel).join(d)) {
                out.push(rel.join(d));
            }
        }
        for l in LOCKS {
            let lock = rel.join(l);
            if std::fs::symlink_metadata(target.join(&lock)).is_ok_and(|m| m.is_file()) {
                out.push(lock);
            }
        }
    };
    for f in [".rustc_info.json", "CACHEDIR.TAG"] {
        if std::fs::symlink_metadata(target.join(f)).is_ok_and(|m| m.is_file()) {
            out.push(PathBuf::from(f));
        }
    }
    let mut names: Vec<String> = std::fs::read_dir(target)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    for n in names {
        let top = target.join(&n);
        if is_profile_dir(&top) {
            profile(PathBuf::from(&n), &mut out);
        } else if !not_a_triple(&top, &n) {
            let mut subs: Vec<String> = std::fs::read_dir(&top)
                .into_iter()
                .flatten()
                .flatten()
                .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            subs.sort();
            for s in subs {
                if is_profile_dir(&top.join(&s)) {
                    profile(Path::new(&n).join(&s), &mut out);
                }
            }
        }
    }
    out
}

// --------------------------------------------------------------- copy

/// The type of the file system that holds `p` (or the nearest directory
/// above it that exists) is APFS, which clones.
#[cfg(target_os = "macos")]
fn is_apfs(p: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let mut p = p.to_path_buf();
    while !p.exists() {
        if !p.pop() {
            return false;
        }
    }
    let Ok(c) = std::ffi::CString::new(p.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `c` is a valid C string and `st` is written by statfs.
    unsafe {
        let mut st: libc::statfs = std::mem::zeroed();
        if libc::statfs(c.as_ptr(), &mut st) != 0 {
            return false;
        }
        let name = std::ffi::CStr::from_ptr(st.f_fstypename.as_ptr());
        name.to_bytes() == b"apfs"
    }
}

#[cfg(not(target_os = "macos"))]
fn is_apfs(_: &Path) -> bool {
    false
}

/// One copy: a directory (its content, into `to`, which is made) or a file.
struct Piece {
    from: PathBuf,
    to: PathBuf,
    dir: bool,
}

/// Clone-or-copy the `pieces`, keeping the times of the files: cargo's
/// freshness check compares them. `size` is what they hold together.
///
/// A clone shares the blocks and costs next to nothing. On macOS `cp -c`
/// falls back to a full copy by itself when it cannot clone, so a clone is
/// counted on only when both sides are on one APFS volume, and otherwise
/// `size` must be under the limit. On Linux the clone is asked for with
/// `--reflink=always` and the plain copy is only tried under the limit.
fn copy_pieces(ctx: &ExecContext, s: &Settings, pieces: &[Piece], size: u64) -> Result<(), String> {
    let Some(first) = pieces.first() else {
        return Ok(());
    };
    let cp = |flags: &[&str], p: &Piece| -> Result<(), String> {
        let (from, to) = if p.dir {
            std::fs::create_dir_all(&p.to).map_err(|e| format!("{}: {e}", p.to.display()))?;
            (p.from.join("."), p.to.clone())
        } else {
            if let Some(parent) = p.to.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("{}: {e}", parent.display()))?;
            }
            (p.from.clone(), p.to.clone())
        };
        let out = ctx
            .command("cp")
            .args(flags)
            .arg(from)
            .arg(to)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .output()
            .map_err(|e| format!("cp: {e}"))?;
        if out.status.success() {
            Ok(())
        } else {
            Err(format!(
                "cp: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ))
        }
    };
    let flags: &[&str] = if cfg!(target_os = "macos") {
        if size > s.copy_limit && !(same_device(&first.from, &first.to) && is_apfs(&first.to)) {
            return Err(format!(
                "no copy-on-write clone here (not one APFS volume) and the copy would take {} MiB",
                size >> 20
            ));
        }
        &["-c", "-pR"]
    } else {
        match cp(&["--reflink=always", "-pR"], first) {
            Ok(()) => &["--reflink=always", "-pR"],
            Err(why) if size > s.copy_limit => {
                return Err(format!(
                    "no copy-on-write clone here ({why}) and the copy would take {} MiB",
                    size >> 20
                ))
            }
            Err(_) => {
                cp(&["-pR"], first)?;
                &["-pR"]
            }
        }
    };
    for (n, p) in pieces.iter().enumerate() {
        // on Linux the first one is done
        if n == 0 && !cfg!(target_os = "macos") {
            continue;
        }
        cp(flags, p)?;
    }
    Ok(())
}

fn same_device(a: &Path, b: &Path) -> bool {
    // `to` does not exist yet: its nearest parent is on the device it will be on
    let dev = |p: &Path| {
        let mut p = p.to_path_buf();
        while !p.exists() {
            if !p.pop() {
                return None;
            }
        }
        std::fs::metadata(p).map(|m| m.dev()).ok()
    };
    matches!((dev(a), dev(b)), (Some(x), Some(y)) if x == y)
}

fn marker_size(entry: &Path) -> Option<u64> {
    std::fs::read_to_string(entry.join(MARKER))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Record a use of the entry now.
fn touch(entry: &Path) {
    let marker = entry.join(MARKER);
    if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&marker) {
        let _ = f.set_modified(SystemTime::now());
    }
}

// ------------------------------------------------------------ restore

#[derive(Debug, PartialEq)]
pub enum Restore {
    /// `target/` is filled from the entry
    Restored,
    /// no entry for the key
    Miss,
    /// not done, and why (`target/` is as it was)
    Skipped(String),
}

/// Put the entry for `key` in `<ws>/target`, when there is an entry and no
/// `target/` yet.
pub fn restore(ctx: &ExecContext, s: &Settings, key: &str, ws: &Path) -> Restore {
    let entry = s.dir.join(key);
    let from = entry.join("target");
    if !from.is_dir() {
        return Restore::Miss;
    }
    let to = ws.join("target");
    if std::fs::symlink_metadata(&to).is_ok() {
        return Restore::Skipped("target/ exists".into());
    }
    let size = marker_size(&entry).unwrap_or(0);
    let piece = Piece {
        from,
        to: to.clone(),
        dir: true,
    };
    match copy_pieces(ctx, s, &[piece], size) {
        Ok(()) => {
            touch(&entry);
            Restore::Restored
        }
        Err(why) => {
            // `to` did not exist before: what is there is a part of the copy
            crate::git::remove_now(&to);
            Restore::Skipped(why)
        }
    }
}

// --------------------------------------------------------------- locks

/// The lock a writer holds on `<tmp>.lock` while it saves.
struct TmpLock {
    path: PathBuf,
    _file: std::fs::File,
}

fn lock_path(tmp: &Path) -> PathBuf {
    let mut p = tmp.as_os_str().to_os_string();
    p.push(".lock");
    PathBuf::from(p)
}

impl TmpLock {
    fn acquire(tmp: &Path) -> std::io::Result<TmpLock> {
        let path = lock_path(tmp);
        for _ in 0..3 {
            let f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)?;
            fs2::FileExt::try_lock_exclusive(&f)?;
            // a prune may have removed the file between open and lock
            if std::fs::metadata(&path)
                .is_ok_and(|m| m.ino() == f.metadata().map_or(0, |m| m.ino()))
            {
                return Ok(TmpLock { path, _file: f });
            }
        }
        Err(std::io::Error::other("the lock file keeps going away"))
    }
}

impl Drop for TmpLock {
    fn drop(&mut self) {
        // before the lock is let go
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A temporary entry whose writer is gone, with the lock held so that
/// nothing else takes it.
struct Dead {
    lock: Option<std::fs::File>,
    lock_path: PathBuf,
}

/// `Some` when nothing writes `tmp`: its lock can be taken, or it has none
/// (a directory from a version that did not lock) and is `old` enough or
/// `any_age` is set.
fn dead_tmp(tmp: &Path, any_age: bool) -> Option<Dead> {
    let lock_path = lock_path(tmp);
    match std::fs::OpenOptions::new().write(true).open(&lock_path) {
        Ok(f) => fs2::FileExt::try_lock_exclusive(&f).ok().map(|()| Dead {
            lock: Some(f),
            lock_path,
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let old = std::fs::metadata(tmp)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.elapsed().ok())
                .is_some_and(|age| age > STALE_TMP);
            (any_age || old).then_some(Dead {
                lock: None,
                lock_path,
            })
        }
        Err(_) => None,
    }
}

impl Dead {
    fn done(self) {
        if self.lock.is_some() {
            let _ = std::fs::remove_file(&self.lock_path);
        }
    }
}

// --------------------------------------------------------------- save

#[derive(Debug, PartialEq)]
pub enum Save {
    Saved,
    /// the key already has an entry (kept as it is)
    Exists,
    /// not done, and why
    Skipped(String),
}

/// Add what cargo owns in the `target/` of `ws` ([`allowed`]) as the entry for
/// `key`, if there is none. `seq` tells apart the temporary names of the
/// threads of one process.
pub fn save(ctx: &ExecContext, s: &Settings, key: &str, ws: &Path, seq: usize) -> Save {
    let entry = s.dir.join(key);
    if entry.exists() {
        return Save::Exists;
    }
    let from = ws.join("target");
    if !crate::git::is_real_dir(&from) {
        return Save::Skipped("no target/".into());
    }
    let rels = allowed(&from);
    // the files at the root of target/ alone would hold the key for nothing
    if !rels.iter().any(|r| r.components().count() > 1) {
        return Save::Skipped("no build of cargo's in target/".into());
    }
    // the size of what is copied, walked once
    let size: u64 = rels
        .iter()
        .map(|r| crate::reclaim::disk_usage(&from.join(r)))
        .sum();
    if size > s.max_bytes {
        return Save::Skipped(format!(
            "{} MiB is more than the cap of the cache",
            size >> 20
        ));
    }
    if let Err(e) = std::fs::create_dir_all(&s.dir) {
        return Save::Skipped(format!("{}: {e}", s.dir.display()));
    }
    let tmp = s
        .dir
        .join(format!("{key}.tmp-{}-{seq}", std::process::id()));
    let _lock = match TmpLock::acquire(&tmp) {
        Ok(l) => l,
        Err(e) => return Save::Skipped(format!("lock: {e}")),
    };
    // held until this returns; the entry is renamed or moved away before
    let fail = |why: String| {
        if tmp.exists() {
            let _ = crate::git::move_to_trash(&tmp, &s.trash);
        }
        Save::Skipped(why)
    };
    if tmp.exists() {
        // a dead writer's of the same name
        if crate::git::move_to_trash(&tmp, &s.trash).is_none() {
            return Save::Skipped(format!("{}: cannot be moved to the trash", tmp.display()));
        }
    }
    if let Err(e) = std::fs::create_dir(&tmp) {
        return Save::Skipped(format!("{}: {e}", tmp.display()));
    }
    let pieces: Vec<Piece> = rels
        .iter()
        .map(|r| Piece {
            from: from.join(r),
            to: tmp.join("target").join(r),
            dir: std::fs::symlink_metadata(from.join(r)).is_ok_and(|m| m.is_dir()),
        })
        .collect();
    if let Err(why) = copy_pieces(ctx, s, &pieces, size) {
        return fail(why);
    }
    if let Err(e) = std::fs::write(tmp.join(MARKER), size.to_string()) {
        return fail(format!("{MARKER}: {e}"));
    }
    match std::fs::rename(&tmp, &entry) {
        Ok(()) => Save::Saved,
        // another job saved the key first
        Err(_) if entry.exists() => {
            let _ = fail(String::new());
            Save::Exists
        }
        Err(e) => fail(format!("rename: {e}")),
    }
}

// ------------------------------------------------------------- listing

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub key: String,
    pub bytes: u64,
    pub used: SystemTime,
}

fn is_tmp(name: &str) -> bool {
    name.contains(".tmp-")
}

/// Whether `name` is that of an entry ([`key`]: 64 lowercase hex digits) or
/// of a temporary one (`<key>.tmp-<pid>-<seq>`), and nothing else.
pub fn is_entry_name(name: &str) -> bool {
    let is_key =
        |s: &str| s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    let digits = |s: &str| !s.is_empty() && s.len() <= 20 && s.bytes().all(|b| b.is_ascii_digit());
    match name.split_once(".tmp-") {
        None => is_key(name),
        Some((key, rest)) => {
            is_key(key)
                && rest
                    .split_once('-')
                    .is_some_and(|(pid, seq)| digits(pid) && digits(seq))
        }
    }
}

/// The entries of the cache (not the temporary ones, nor anything that is
/// not an entry: those have the marker file), as they are on disk.
pub fn entries(dir: &Path) -> Vec<Entry> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if is_tmp(&name) || !e.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let path = e.path();
        if !path.join(MARKER).is_file() {
            continue;
        }
        let used = std::fs::metadata(path.join(MARKER))
            .and_then(|m| m.modified())
            .or_else(|_| e.metadata().and_then(|m| m.modified()))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let bytes = marker_size(&path).unwrap_or_else(|| crate::reclaim::disk_usage(&path));
        v.push(Entry {
            key: name,
            bytes,
            used,
        });
    }
    v
}

/// The keys to remove so that the rest, with `extra` bytes that are not
/// entries (temporary ones), fits in `max_bytes`: the least recently used
/// first. `keep` is never chosen.
pub fn lru_victims(
    entries: &[Entry],
    extra: u64,
    max_bytes: u64,
    keep: Option<&str>,
) -> Vec<String> {
    let mut total: u64 = extra + entries.iter().map(|e| e.bytes).sum::<u64>();
    let mut by_age: Vec<&Entry> = entries.iter().collect();
    by_age.sort_by(|a, b| a.used.cmp(&b.used).then_with(|| a.key.cmp(&b.key)));
    let mut out = Vec::new();
    for e in by_age {
        if total <= max_bytes {
            break;
        }
        if keep == Some(e.key.as_str()) {
            continue;
        }
        total = total.saturating_sub(e.bytes);
        out.push(e.key.clone());
    }
    out
}

/// What [`prune`] and [`clear`] did.
#[derive(Debug, Default, PartialEq)]
pub struct Pruned {
    /// entries (and dead temporary ones) moved to the trash
    pub removed: usize,
    /// what could not be done: those stay where they are
    pub warnings: Vec<String>,
    /// the entry that was just saved had to go too: it alone was over the cap
    pub evicted_keep: bool,
}

/// Move `path` to the trash and start emptying it. Nothing is deleted in
/// place: when the move fails the entry stays.
fn remove_entry(ctx: &ExecContext, s: &Settings, path: &Path) -> Result<(), String> {
    if crate::git::move_to_trash(path, &s.trash).is_none() {
        return Err(format!("{}: cannot be moved to the trash", path.display()));
    }
    crate::git::empty_trash(ctx, &s.trash);
    Ok(())
}

/// Reap the temporary entries of writers that are gone (and their lock files).
/// Returns what the ones still being written take.
fn reap_tmps(ctx: &ExecContext, s: &Settings, any_age: bool, out: &mut Pruned) -> u64 {
    let mut live = 0;
    for e in std::fs::read_dir(&s.dir).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if !is_tmp(&name) {
            continue;
        }
        let path = e.path();
        if let Some(base) = name.strip_suffix(".lock") {
            // a lock file whose temporary entry is gone
            if !s.dir.join(base).exists() {
                if let Some(d) = dead_tmp(&s.dir.join(base), true) {
                    d.done();
                }
            }
            continue;
        }
        if !e.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        match dead_tmp(&path, any_age) {
            Some(d) => match remove_entry(ctx, s, &path) {
                Ok(()) => {
                    out.removed += 1;
                    d.done();
                }
                Err(w) => out.warnings.push(w),
            },
            None => live += crate::reclaim::disk_usage(&path),
        }
    }
    live
}

/// Remove the least recently used entries until the cache (the temporary
/// entries being written count) fits its cap, and the temporary entries of
/// jobs that died. When only the entry `keep` is left and the cache is still
/// over the cap, that goes too (`evicted_keep`).
pub fn prune(ctx: &ExecContext, s: &Settings, keep: Option<&str>) -> Pruned {
    let mut out = Pruned::default();
    let live = reap_tmps(ctx, s, false, &mut out);
    let es = entries(&s.dir);
    let mut total: u64 = live + es.iter().map(|e| e.bytes).sum::<u64>();
    for k in lru_victims(&es, live, s.max_bytes, keep) {
        match remove_entry(ctx, s, &s.dir.join(&k)) {
            Ok(()) => {
                out.removed += 1;
                total = total.saturating_sub(es.iter().find(|e| e.key == k).map_or(0, |e| e.bytes));
            }
            Err(w) => out.warnings.push(w),
        }
    }
    if total > s.max_bytes {
        if let Some(k) = keep.filter(|k| s.dir.join(k).is_dir()) {
            match remove_entry(ctx, s, &s.dir.join(k)) {
                Ok(()) => {
                    out.removed += 1;
                    out.evicted_keep = true;
                    out.warnings.push(format!(
                        "the cache is over its cap ({} MiB) with the new entry alone: it is not kept",
                        s.max_bytes >> 20
                    ));
                }
                Err(w) => out.warnings.push(w),
            }
        }
    }
    out
}

/// Remove every entry and every temporary entry whose writer is gone
/// (`local-ci cache prune --build`). Nothing else in the directory is touched.
pub fn clear(ctx: &ExecContext, s: &Settings) -> Pruned {
    let mut out = Pruned::default();
    reap_tmps(ctx, s, true, &mut out);
    for e in entries(&s.dir) {
        match remove_entry(ctx, s, &s.dir.join(&e.key)) {
            Ok(()) => out.removed += 1,
            Err(w) => out.warnings.push(w),
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests build their own commands
mod tests {
    use super::*;

    fn inputs() -> KeyInputs {
        KeyInputs {
            rustc: "rustc 1.100.0".into(),
            cargo: "cargo 1.100.0".into(),
            os_arch: "macos-aarch64".into(),
            locks: vec![("Cargo.lock".into(), "aa".into())],
            job: ".github/workflows/ci.yml#test".into(),
            env: vec![("RUSTFLAGS".into(), "-D warnings".into())],
            files: vec![("rust-toolchain.toml".into(), "t1".into())],
            matrix: "{\"os\":\"linux\"}".into(),
        }
    }

    #[test]
    fn the_key_is_stable_and_changes_with_each_input() {
        let base = key(&inputs());
        assert_eq!(base, key(&inputs()));
        assert_eq!(base.len(), 64);
        let changed = [
            KeyInputs {
                rustc: "rustc 1.101.0".into(),
                ..inputs()
            },
            KeyInputs {
                cargo: "cargo 1.101.0".into(),
                ..inputs()
            },
            KeyInputs {
                os_arch: "linux-x86_64".into(),
                ..inputs()
            },
            KeyInputs {
                locks: vec![("Cargo.lock".into(), "bb".into())],
                ..inputs()
            },
            KeyInputs {
                locks: vec![
                    ("Cargo.lock".into(), "aa".into()),
                    ("x/Cargo.lock".into(), "aa".into()),
                ],
                ..inputs()
            },
            KeyInputs {
                job: ".github/workflows/ci.yml#lint".into(),
                ..inputs()
            },
            KeyInputs {
                env: vec![("RUSTFLAGS".into(), "-C opt-level=3".into())],
                ..inputs()
            },
            KeyInputs {
                env: vec![],
                ..inputs()
            },
            KeyInputs {
                files: vec![("rust-toolchain.toml".into(), "t2".into())],
                ..inputs()
            },
            KeyInputs {
                files: vec![],
                ..inputs()
            },
            KeyInputs {
                files: vec![
                    ("rust-toolchain.toml".into(), "t1".into()),
                    (".cargo/config.toml".into(), "c".into()),
                ],
                ..inputs()
            },
            KeyInputs {
                matrix: "{\"os\":\"macos\"}".into(),
                ..inputs()
            },
        ];
        for (n, c) in changed.iter().enumerate() {
            assert_ne!(base, key(c), "input {n} did not change the key");
        }
    }

    #[test]
    fn parts_do_not_run_together() {
        let a = KeyInputs {
            rustc: "ab".into(),
            cargo: "c".into(),
            ..KeyInputs::default()
        };
        let b = KeyInputs {
            rustc: "a".into(),
            cargo: "bc".into(),
            ..KeyInputs::default()
        };
        assert_ne!(key(&a), key(&b));
    }

    #[test]
    fn only_the_variables_that_change_a_build_are_in_the_key() {
        let ws = Path::new("/w/run1");
        let vars = [
            ("RUSTFLAGS", "-C link-arg=-L/w/run1/lib"),
            ("RUSTDOCFLAGS", "-D warnings"),
            ("CARGO_INCREMENTAL", "0"),
            ("CARGO_BUILD_JOBS", "4"),
            ("NEXTEST_TEST_THREADS", "4"),
            ("RUST_TEST_THREADS", "4"),
            ("CARGO_TERM_COLOR", "always"),
            ("RUSTC_WRAPPER", "sccache"),
            ("LLVM_PROFILE_FILE", "x.profraw"),
            ("CARGO_LLVM_COV_TARGET_DIR", "t"),
            ("PATH", "/usr/bin"),
            ("HOME", "/h"),
            ("GITHUB_SHA", "abc"),
        ];
        let env = build_env(vars, ws);
        let names: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            names,
            [
                "CARGO_INCREMENTAL",
                "CARGO_LLVM_COV_TARGET_DIR",
                "LLVM_PROFILE_FILE",
                "RUSTC_WRAPPER",
                "RUSTDOCFLAGS",
                "RUSTFLAGS"
            ]
        );
        // the path of the worktree is not part of a value
        assert_eq!(
            env.iter().find(|(k, _)| k == "RUSTFLAGS").unwrap().1,
            "-C link-arg=-L<workspace>/lib"
        );
        // and the same flags from another worktree give the same key
        let other = build_env(
            [("RUSTFLAGS", "-C link-arg=-L/w/run2/lib")],
            Path::new("/w/run2"),
        );
        assert_eq!(
            other[0].1,
            env.iter().find(|(k, _)| k == "RUSTFLAGS").unwrap().1
        );
        assert!(!moves_target(&env));
        assert!(moves_target(&build_env([("CARGO_TARGET_DIR", "/t")], ws)));
    }

    #[test]
    fn only_the_lock_at_the_root_counts() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path();
        assert!(lock_digests(p).is_empty());
        std::fs::write(p.join("Cargo.lock"), "a").unwrap();
        std::fs::create_dir_all(p.join("sub")).unwrap();
        std::fs::write(p.join("sub/Cargo.lock"), "b").unwrap();
        let locks = lock_digests(p);
        let names: Vec<&str> = locks.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["Cargo.lock"]);
        // the content is what counts
        std::fs::write(p.join("Cargo.lock"), "changed").unwrap();
        assert_ne!(locks[0].1, lock_digests(p)[0].1);
        assert!(applies(p) == p.join("Cargo.toml").is_file());
    }

    fn touch_files(root: &Path, files: &[&str]) {
        for f in files {
            let p = root.join(f);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, "x").unwrap();
        }
    }

    #[test]
    fn only_what_cargo_owns_is_allowed() {
        let t = tempfile::tempdir().unwrap();
        let d = t.path().join("target");
        touch_files(
            &d,
            &[
                "CACHEDIR.TAG",
                ".rustc_info.json",
                "debug/.fingerprint/a/x",
                "debug/deps/libfoo.rlib",
                "debug/build/b/out",
                "debug/.cargo-lock",
                "debug/incremental/foo-1/a.o",
                // the layout of the newer cargo: no deps, no .fingerprint
                "bench/.cargo-build-lock",
                "bench/build/p/h/out/x",
                "bench/examples/e",
                "debug/examples/ex",
                "debug/myapp",
                "release/.fingerprint/a/x",
                "release/deps/libfoo.rlib",
                "x86_64-unknown-linux-gnu/debug/.fingerprint/a/x",
                "x86_64-unknown-linux-gnu/debug/deps/libfoo.rlib",
                "x86_64-unknown-linux-gnu/debug/incremental/i",
                "llvm-cov-target/debug/.fingerprint/a/x",
                "llvm-cov-target/CACHEDIR.TAG",
                // a target directory of a tool, whatever its name
                "somename/CACHEDIR.TAG",
                "somename/debug/.fingerprint/a/x",
                "llvm-cov-target/debug/deps/default_1.profraw",
                "nextest/default/junit.xml",
                "tmp/x",
                "criterion/report/index.html",
                "doc/foo/index.html",
                "mutants.out/log",
                "lcov.info",
                "default.profdata",
            ],
        );
        let got: Vec<String> = allowed(&d)
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        let want = [
            ".rustc_info.json",
            "CACHEDIR.TAG",
            "debug/deps",
            "debug/build",
            "debug/.fingerprint",
            "debug/.cargo-lock",
            "bench/build",
            "bench/.cargo-build-lock",
            "release/deps",
            "release/.fingerprint",
            "x86_64-unknown-linux-gnu/debug/deps",
            "x86_64-unknown-linux-gnu/debug/.fingerprint",
        ];
        let mut got_sorted = got.clone();
        got_sorted.sort();
        let mut want_sorted: Vec<String> = want.iter().map(|w| w.to_string()).collect();
        want_sorted.sort();
        assert_eq!(got_sorted, want_sorted, "{got:?}");
    }

    fn entry(key: &str, bytes: u64, age: u64) -> Entry {
        Entry {
            key: key.into(),
            bytes,
            used: SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000 - age),
        }
    }

    #[test]
    fn the_least_recently_used_entries_go_first() {
        let es = [
            entry("a", 10, 300),
            entry("b", 10, 100),
            entry("c", 10, 200),
        ];
        assert!(lru_victims(&es, 0, 30, None).is_empty());
        assert_eq!(lru_victims(&es, 0, 20, None), ["a"]);
        assert_eq!(lru_victims(&es, 0, 10, None), ["a", "c"]);
        assert_eq!(lru_victims(&es, 0, 0, None), ["a", "c", "b"]);
        // the entry just saved stays
        assert_eq!(lru_victims(&es, 0, 20, Some("a")), ["c"]);
        // what is being written counts
        assert_eq!(lru_victims(&es, 10, 30, None), ["a"]);
    }

    #[test]
    fn the_summary_counts_jobs() {
        let j = |r, s| JobCache {
            key: "k".into(),
            restored: r,
            saved: s,
        };
        assert_eq!(summary([j(false, false)].iter()), "");
        assert_eq!(
            summary([j(true, false), j(true, true), j(false, false)].iter()),
            "build cache  restored 2 · saved 1"
        );
    }

    fn ctx() -> ExecContext {
        ExecContext::new(
            [("PATH", "/usr/bin:/bin")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn settings(root: &Path) -> Settings {
        Settings {
            dir: root.join("cache/build"),
            trash: root.join("cache/trash"),
            max_bytes: u64::MAX,
            copy_limit: u64::MAX,
            probe_timeout: Duration::from_secs(20),
        }
    }

    /// A target with what cargo owns and what it does not.
    fn make_target(ws: &Path) {
        touch_files(
            &ws.join("target"),
            &[
                "debug/.fingerprint/dep-1/x",
                "debug/deps/lib.rlib",
                "debug/incremental/x",
                "tmp/x",
                "nextest/y",
            ],
        );
        std::fs::write(ws.join("target/debug/deps/lib.rlib"), "lib").unwrap();
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .map(|d| {
                d.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    }

    #[test]
    fn a_saved_target_is_restored_without_what_cargo_does_not_own() {
        let t = tempfile::tempdir().unwrap();
        let s = settings(t.path());
        let (a, b) = (t.path().join("a"), t.path().join("b"));
        make_target(&a);
        std::fs::create_dir_all(&b).unwrap();
        let c = ctx();
        assert_eq!(restore(&c, &s, "k", &b), Restore::Miss);
        assert_eq!(save(&c, &s, "k", &a, 0), Save::Saved);
        // the job's own target is as it was
        assert!(a.join("target/debug/incremental/x").exists());
        assert_eq!(save(&c, &s, "k", &a, 1), Save::Exists);
        assert_eq!(restore(&c, &s, "k", &b), Restore::Restored);
        assert_eq!(
            std::fs::read_to_string(b.join("target/debug/deps/lib.rlib")).unwrap(),
            "lib"
        );
        assert!(b.join("target/debug/.fingerprint/dep-1/x").exists());
        assert!(!b.join("target/debug/incremental").exists());
        assert!(!b.join("target/tmp").exists());
        assert!(!b.join("target/nextest").exists());
        // not over a target that is there
        assert!(matches!(restore(&c, &s, "k", &b), Restore::Skipped(_)));
        // no temporary entry, and no lock file, is left
        assert_eq!(names(&s.dir), ["k"]);
        let es = entries(&s.dir);
        assert_eq!(es.len(), 1);
        assert!(es[0].bytes > 0);
    }

    #[test]
    fn a_target_without_what_cargo_owns_is_not_saved() {
        let t = tempfile::tempdir().unwrap();
        let s = settings(t.path());
        let a = t.path().join("a");
        touch_files(&a.join("target"), &["tmp/x"]);
        assert!(matches!(save(&ctx(), &s, "k", &a, 0), Save::Skipped(_)));
        // the files at the root alone are not an entry either
        touch_files(&a.join("target"), &["CACHEDIR.TAG", ".rustc_info.json"]);
        assert!(matches!(save(&ctx(), &s, "k", &a, 0), Save::Skipped(_)));
        assert!(!s.dir.join("k").exists());
    }

    #[test]
    fn a_copy_that_is_not_a_clone_is_skipped_above_the_limit() {
        let t = tempfile::tempdir().unwrap();
        let mut s = settings(t.path());
        let a = t.path().join("a");
        make_target(&a);
        std::fs::write(a.join("target/debug/deps/big"), vec![0u8; 8192]).unwrap();
        s.copy_limit = 0;
        let c = ctx();
        // on a file system that clones, this is saved; on one that does not, skipped
        // and nothing is left behind either way
        match save(&c, &s, "k", &a, 0) {
            Save::Saved => assert!(s.dir.join("k").is_dir()),
            Save::Skipped(_) => assert!(!s.dir.join("k").exists()),
            Save::Exists => unreachable!(),
        }
        assert!(names(&s.dir).iter().all(|n| !n.contains(".tmp-")));
    }

    fn fake_entry(s: &Settings, k: &str, bytes: u64, age: u64) {
        let p = s.dir.join(k);
        std::fs::create_dir_all(p.join("target")).unwrap();
        std::fs::write(p.join(MARKER), bytes.to_string()).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(p.join(MARKER))
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(age))
            .unwrap();
    }

    #[test]
    fn what_the_cache_moves_to_the_trash_is_emptied_from_it() {
        let t = tempfile::tempdir().unwrap();
        let s = settings(t.path());
        let k = key(&inputs());
        let tmp = format!("{k}.tmp-{}-3", std::process::id());
        let mut moved = Vec::new();
        for name in [k.as_str(), tmp.as_str()] {
            let p = s.dir.join(name);
            std::fs::create_dir_all(p.join("target/debug")).unwrap();
            moved.push(crate::git::move_to_trash(&p, &s.trash).unwrap());
        }
        crate::git::empty_trash_now(&s.trash);
        for p in &moved {
            assert!(!p.exists(), "{} is left in the trash", p.display());
        }
    }

    #[test]
    fn only_the_names_of_entries_are_entry_names() {
        let k = "a".repeat(64);
        assert!(is_entry_name(&k));
        assert!(is_entry_name(&format!("{k}.tmp-42-0")));
        for not in [
            "a".repeat(63),
            "a".repeat(65),
            "A".repeat(64),
            "g".repeat(64),
            format!("{k}.tmp-"),
            format!("{k}.tmp-42"),
            format!("{k}.tmp-x-0"),
            format!("{k}.old"),
            "old".to_string(),
        ] {
            assert!(!is_entry_name(&not), "{not}");
        }
    }

    #[test]
    fn prune_removes_the_oldest_to_fit_and_the_dead_temporaries() {
        let t = tempfile::tempdir().unwrap();
        let mut s = settings(t.path());
        std::fs::create_dir_all(&s.dir).unwrap();
        fake_entry(&s, "old", 1000, 100);
        fake_entry(&s, "new", 1000, 10);
        // a temporary entry of a version that did not lock, old
        let dead = s.dir.join("zz.tmp-1-0");
        std::fs::create_dir_all(&dead).unwrap();
        std::fs::File::open(&dead)
            .unwrap()
            .set_modified(SystemTime::now() - 2 * STALE_TMP)
            .unwrap();
        // one that is young, without a lock
        let young = s.dir.join("yy.tmp-2-0");
        std::fs::create_dir_all(&young).unwrap();
        // the young temporary entry counts by its size on disk, which differs
        // by platform (an empty directory takes 0 blocks on APFS, 4 KiB on
        // ext4): the cap leaves room for it and one 1000-byte entry, not two
        s.max_bytes = 1500 + crate::reclaim::disk_usage(&young);
        let c = ctx();
        let p = prune(&c, &s, None);
        assert_eq!(p.removed, 2, "{p:?}");
        assert!(p.warnings.is_empty(), "{p:?}");
        assert!(!s.dir.join("old").exists());
        assert!(s.dir.join("new").exists());
        assert!(!dead.exists());
        assert!(young.exists());
        // a file that is not the cache's is not touched by clear
        std::fs::write(s.dir.join("foreign.txt"), "x").unwrap();
        std::fs::create_dir_all(s.dir.join("foreign-dir")).unwrap();
        let p = clear(&c, &s);
        // `new` and the young temporary entry (no writer holds it)
        assert_eq!(p.removed, 2, "{p:?}");
        assert!(entries(&s.dir).is_empty());
        assert!(!young.exists());
        assert!(s.dir.join("foreign.txt").exists());
        assert!(s.dir.join("foreign-dir").exists());
    }

    #[test]
    fn a_temporary_entry_that_a_writer_holds_is_not_reaped() {
        let t = tempfile::tempdir().unwrap();
        let mut s = settings(t.path());
        std::fs::create_dir_all(&s.dir).unwrap();
        let tmp = s.dir.join("k.tmp-9-0");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join("f"), "x").unwrap();
        // old by every clock
        std::fs::File::open(&tmp)
            .unwrap()
            .set_modified(SystemTime::now() - 2 * STALE_TMP)
            .unwrap();
        let lock = TmpLock::acquire(&tmp).unwrap();
        let c = ctx();
        assert_eq!(prune(&c, &s, None).removed, 0);
        assert!(tmp.join("f").exists());
        assert_eq!(clear(&c, &s).removed, 0);
        assert!(tmp.join("f").exists());
        // what it holds counts against the cap
        fake_entry(&s, "e", 1000, 10);
        s.max_bytes = 1000;
        assert!(crate::reclaim::disk_usage(&tmp) > 0);
        assert_eq!(prune(&c, &s, None).removed, 1);
        assert!(!s.dir.join("e").exists());
        // the writer is done: the lock goes and the next prune reaps it
        drop(lock);
        assert!(!lock_path(&tmp).exists());
        assert!(tmp.exists());
        // no lock file and old: reaped by age
        assert_eq!(prune(&c, &s, None).removed, 1);
        assert!(!tmp.exists());
    }

    #[test]
    fn a_stale_unlocked_temporary_entry_is_reaped_with_its_lock_file() {
        let t = tempfile::tempdir().unwrap();
        let s = settings(t.path());
        std::fs::create_dir_all(&s.dir).unwrap();
        let tmp = s.dir.join("k.tmp-9-0");
        std::fs::create_dir_all(&tmp).unwrap();
        // the writer died: the file is there, nobody holds it. Young by the clock.
        std::fs::write(lock_path(&tmp), "").unwrap();
        // and a lock file whose directory is gone
        let orphan = lock_path(&s.dir.join("j.tmp-9-1"));
        std::fs::write(&orphan, "").unwrap();
        let p = prune(&ctx(), &s, None);
        assert_eq!(p.removed, 1, "{p:?}");
        assert!(!tmp.exists());
        assert!(!lock_path(&tmp).exists());
        assert!(!orphan.exists());
    }

    #[test]
    fn a_cache_still_over_its_cap_drops_the_entry_just_saved() {
        let t = tempfile::tempdir().unwrap();
        let mut s = settings(t.path());
        std::fs::create_dir_all(&s.dir).unwrap();
        fake_entry(&s, "other", 400, 100);
        fake_entry(&s, "fresh", 1000, 0);
        s.max_bytes = 500;
        let p = prune(&ctx(), &s, Some("fresh"));
        assert!(p.evicted_keep, "{p:?}");
        assert_eq!(p.removed, 2);
        assert_eq!(p.warnings.len(), 1);
        assert!(entries(&s.dir).is_empty());
        // within the cap: kept
        fake_entry(&s, "fresh", 400, 0);
        let p = prune(&ctx(), &s, Some("fresh"));
        assert!(!p.evicted_keep && p.removed == 0, "{p:?}");
    }

    #[test]
    fn an_entry_that_cannot_be_moved_to_the_trash_stays() {
        let t = tempfile::tempdir().unwrap();
        let mut s = settings(t.path());
        std::fs::create_dir_all(&s.dir).unwrap();
        fake_entry(&s, "old", 1000, 100);
        fake_entry(&s, "new", 1000, 10);
        // the trash is a file: nothing can be moved into it
        std::fs::write(&s.trash, "").unwrap();
        s.max_bytes = 1000;
        let p = prune(&ctx(), &s, None);
        assert_eq!(p.removed, 0, "{p:?}");
        assert_eq!(p.warnings.len(), 1);
        assert!(s.dir.join("old").join("target").is_dir());
        assert_eq!(clear(&ctx(), &s).removed, 0);
        assert!(s.dir.join("new").exists());
    }

    #[test]
    fn restoring_touches_the_entry() {
        let t = tempfile::tempdir().unwrap();
        let s = settings(t.path());
        let (a, b) = (t.path().join("a"), t.path().join("b"));
        make_target(&a);
        std::fs::create_dir_all(&b).unwrap();
        let c = ctx();
        assert_eq!(save(&c, &s, "k", &a, 0), Save::Saved);
        let marker = s.dir.join("k").join(MARKER);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&marker)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(86_400))
            .unwrap();
        assert_eq!(restore(&c, &s, "k", &b), Restore::Restored);
        let age = std::fs::metadata(&marker)
            .unwrap()
            .modified()
            .unwrap()
            .elapsed()
            .unwrap();
        assert!(age < Duration::from_secs(60));
    }

    #[test]
    fn the_environment_sets_the_cache() {
        let dir = Path::new("/c");
        let with = |k: &str, v: &str| {
            ExecContext::new([(k.to_string(), v.to_string())].into_iter().collect())
        };
        let s = Settings::from_ctx(&with("X", "1"), dir, false).unwrap();
        assert_eq!(s.dir, Path::new("/c/build"));
        assert_eq!(s.max_bytes, 20 * 1024 * 1024 * 1024);
        assert_eq!(s.probe_timeout, Duration::from_secs(20));
        assert!(Settings::from_ctx(&with("X", "1"), dir, true).is_none());
        assert!(Settings::from_ctx(&with("LOCAL_CI_NO_BUILD_CACHE", "1"), dir, false).is_none());
        assert!(Settings::from_ctx(&with("LOCAL_CI_NO_BUILD_CACHE", "0"), dir, false).is_some());
        let s =
            Settings::from_ctx(&with("LOCAL_CI_BUILD_CACHE_MAX_GB", "0.5"), dir, false).unwrap();
        assert_eq!(s.max_bytes, 512 * 1024 * 1024);
        // a cap of 0 is a cache that is off (but can still be looked at)
        let zero = with("LOCAL_CI_BUILD_CACHE_MAX_GB", "0");
        assert!(Settings::from_ctx(&zero, dir, false).is_none());
        assert_eq!(Settings::at(&zero, dir).max_bytes, 0);
        let s = Settings::from_ctx(
            &with("LOCAL_CI_BUILD_CACHE_PROBE_TIMEOUT_SECS", "2.5"),
            dir,
            false,
        )
        .unwrap();
        assert_eq!(s.probe_timeout, Duration::from_millis(2500));
    }

    fn fake_tool(dir: &Path, name: &str, body: &str) {
        crate::fake_exec::write_executable(&dir.join(name), format!("#!/bin/sh\n{body}\n"), 0o755);
    }

    #[test]
    fn the_toolchain_probe_is_bounded_and_does_not_install() {
        let t = tempfile::tempdir().unwrap();
        let bin = t.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        // prints what rustup would be told, to see it is told not to install
        fake_tool(&bin, "rustc", "echo \"rustc 1 auto=$RUSTUP_AUTO_INSTALL\"");
        fake_tool(&bin, "cargo", "echo cargo 1");
        let path = format!("{}:/usr/bin:/bin", bin.display());
        let c = ctx();
        let (r, k) = toolchain(&c, t.path(), &path, Duration::from_secs(20)).unwrap();
        assert_eq!(r, "rustc 1 auto=0");
        assert_eq!(k, "cargo 1");
        // too slow
        fake_tool(&bin, "rustc", "sleep 30");
        let started = Instant::now();
        let e = toolchain(&c, t.path(), &path, Duration::from_millis(300)).unwrap_err();
        assert!(e.contains("took more than"), "{e}");
        assert!(started.elapsed() < Duration::from_secs(10));
        // failing
        fake_tool(&bin, "rustc", "exit 3");
        assert!(toolchain(&c, t.path(), &path, Duration::from_secs(20)).is_err());
        // missing
        assert!(toolchain(&c, t.path(), "/nonexistent", Duration::from_secs(20)).is_err());
    }

    #[test]
    fn the_files_that_choose_the_toolchain_are_in_the_key() {
        let t = tempfile::tempdir().unwrap();
        assert!(config_digests(t.path()).is_empty());
        std::fs::create_dir_all(t.path().join(".cargo")).unwrap();
        std::fs::write(t.path().join(".cargo/config.toml"), "a").unwrap();
        std::fs::write(t.path().join("rust-toolchain.toml"), "b").unwrap();
        let one = config_digests(t.path());
        assert_eq!(one.len(), 2);
        std::fs::write(t.path().join(".cargo/config.toml"), "c").unwrap();
        assert_ne!(one, config_digests(t.path()));
    }
}
