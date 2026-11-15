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
//! Entries are immutable: `<dir>/<key>/target`, written under a temporary name
//! and renamed into place, so that two jobs saving the same key cannot mix
//! theirs. The marker file in an entry holds its size, and its mtime is the
//! time of its last use (for the least-recently-used pruning).
//!
//! Nothing here may fail a job: the functions report, and the caller warns.

use crate::context::ExecContext;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime};

/// Changes when what an entry holds, or what its key covers, changes.
pub const SCHEMA: u32 = 1;

/// The file in an entry that holds its size in bytes.
const MARKER: &str = ".local-ci-entry";

/// Directory name (of the cache directory) the entries live in.
pub const DIR: &str = "build";

/// Total size of the cache before the least recently used entries go, in GiB.
const DEFAULT_MAX_GB: f64 = 20.0;

/// The most a copy that is not a clone may take, in MiB. Above it the cache
/// is skipped rather than slow the job down with a copy.
const DEFAULT_COPY_LIMIT_MB: u64 = 512;

/// A temporary entry this old was left by a job that died.
const STALE_TMP: Duration = Duration::from_secs(3600);

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
}

impl Settings {
    /// The settings for a cache directory, from the environment of `ctx`:
    /// `LOCAL_CI_NO_BUILD_CACHE` (anything but empty or `0`) turns the cache
    /// off (`None`), `LOCAL_CI_BUILD_CACHE_MAX_GB` sets the size cap (default
    /// 20) and `LOCAL_CI_BUILD_CACHE_COPY_LIMIT_MB` the most that is copied
    /// where cloning is not available (default 512).
    pub fn from_ctx(ctx: &ExecContext, cache_dir: &Path, disabled: bool) -> Option<Settings> {
        if disabled || ctx.var("LOCAL_CI_NO_BUILD_CACHE").is_some_and(truthy) {
            return None;
        }
        Some(Self::at(ctx, cache_dir))
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
        Settings {
            dir: cache_dir.join(DIR),
            trash: cache_dir.join("trash"),
            max_bytes: (gb * 1024.0 * 1024.0 * 1024.0) as u64,
            copy_limit: mb * 1024 * 1024,
        }
    }
}

fn truthy(v: &str) -> bool {
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
    /// every `Cargo.lock` of the checkout: relative path and the hash of its content
    pub locks: Vec<(String, String)>,
    /// the workflow file and the job id
    pub job: String,
    /// the variables that change a build, see [`build_env`]
    pub env: Vec<(String, String)>,
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

/// Every `Cargo.lock` below `ws` (not in `target`, `.git` or `node_modules`),
/// by relative path, with the hash of its content.
pub fn lock_digests(ws: &Path) -> Vec<(String, String)> {
    fn walk(base: &Path, dir: &Path, depth: usize, out: &mut Vec<(String, String)>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let Ok(t) = e.file_type() else { continue };
            let name = e.file_name();
            let name = name.to_string_lossy();
            if t.is_file() && name == "Cargo.lock" {
                if let Ok(bytes) = std::fs::read(e.path()) {
                    let rel = e
                        .path()
                        .strip_prefix(base)
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    out.push((rel, hex(&Sha256::digest(&bytes))));
                }
            } else if t.is_dir()
                && depth < 4
                && !matches!(name.as_ref(), "target" | ".git" | "node_modules")
            {
                walk(base, &e.path(), depth + 1, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(ws, ws, 0, &mut out);
    out.sort();
    out
}

/// The `rustc -vV` and `cargo -V` of the checkout (so that `rust-toolchain.toml`
/// is honoured), run with `path` as `PATH`. `None` when either is missing.
pub fn toolchain(ctx: &ExecContext, ws: &Path, path: &str) -> Option<(String, String)> {
    let run = |prog: &str, arg: &str| -> Option<String> {
        let out = ctx
            .command(prog)
            .arg(arg)
            .env("PATH", path)
            .current_dir(ws)
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
        (out.status.success() && !text.is_empty()).then_some(text)
    };
    Some((run("rustc", "-vV")?, run("cargo", "-V")?))
}

pub fn os_arch() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

// -------------------------------------------------------------- strip

/// Whether a file or directory found in a `target/` is left out of an entry:
/// incremental compilation data (large, and only good for the checkout that
/// made it), raw coverage data and the output of mutation testing.
pub fn should_strip(name: &str, is_dir: bool) -> bool {
    if is_dir {
        name == "incremental" || name.starts_with("mutants.out")
    } else {
        name.ends_with(".profraw")
    }
}

/// Remove what [`should_strip`] names from below `dir`, links not followed.
pub fn strip(dir: &Path) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let Ok(t) = e.file_type() else { continue };
        let name = e.file_name();
        let name = name.to_string_lossy();
        if t.is_symlink() {
            continue;
        }
        if should_strip(&name, t.is_dir()) {
            if t.is_dir() {
                let _ = std::fs::remove_dir_all(e.path());
            } else {
                let _ = std::fs::remove_file(e.path());
            }
        } else if t.is_dir() {
            strip(&e.path());
        }
    }
}

// --------------------------------------------------------------- copy

/// Clone-or-copy `from` (a directory) to `to` (which must not exist), keeping
/// the times of the files: cargo's freshness check compares them.
///
/// A clone shares the blocks and costs next to nothing. `cp` falls back to a
/// full copy by itself on macOS when the volumes differ, so there the copy
/// is allowed only when the two are on one device or `size` is under the
/// limit; on Linux the clone is asked for with `--reflink=always` and the
/// plain copy is only tried under the limit.
fn clone_dir(
    ctx: &ExecContext,
    s: &Settings,
    from: &Path,
    to: &Path,
    size: u64,
) -> Result<(), String> {
    let cp = |args: &[&str]| -> Result<(), String> {
        let out = ctx
            .command("cp")
            .args(args)
            .arg(from)
            .arg(to)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .output()
            .map_err(|e| format!("cp: {e}"))?;
        if out.status.success() {
            Ok(())
        } else {
            let _ = std::fs::remove_dir_all(to);
            Err(format!(
                "cp: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ))
        }
    };
    if cfg!(target_os = "macos") {
        if size > s.copy_limit && !same_device(from, to) {
            return Err(format!(
                "the cache is on another volume and the copy would take {} MiB",
                size >> 20
            ));
        }
        cp(&["-c", "-pR"])
    } else {
        match cp(&["--reflink=always", "-pR"]) {
            Ok(()) => Ok(()),
            Err(why) if size > s.copy_limit => Err(format!(
                "no copy-on-write clone here ({why}) and the copy would take {} MiB",
                size >> 20
            )),
            Err(_) => cp(&["-pR"]),
        }
    }
}

fn same_device(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    // `to` does not exist yet: its parent is on the device it will be on
    let dev = |p: &Path| {
        let p = if p.exists() {
            p.to_path_buf()
        } else {
            p.parent().map(Path::to_path_buf).unwrap_or_default()
        };
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
    match clone_dir(ctx, s, &from, &to, size) {
        Ok(()) => {
            touch(&entry);
            Restore::Restored
        }
        Err(why) => Restore::Skipped(why),
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

/// Add the `target/` of `ws` as the entry for `key`, if there is none. `seq`
/// tells apart the temporary names of the threads of one process.
pub fn save(ctx: &ExecContext, s: &Settings, key: &str, ws: &Path, seq: usize) -> Save {
    let entry = s.dir.join(key);
    if entry.exists() {
        return Save::Exists;
    }
    let from = ws.join("target");
    if !crate::git::is_real_dir(&from) {
        return Save::Skipped("no target/".into());
    }
    let tmp = s
        .dir
        .join(format!("{key}.tmp-{}-{seq}", std::process::id()));
    if let Err(e) = std::fs::create_dir_all(&s.dir) {
        return Save::Skipped(format!("{}: {e}", s.dir.display()));
    }
    let _ = std::fs::remove_dir_all(&tmp);
    if let Err(e) = std::fs::create_dir(&tmp) {
        return Save::Skipped(format!("{}: {e}", tmp.display()));
    }
    let fail = |why: String| {
        let _ = std::fs::remove_dir_all(&tmp);
        Save::Skipped(why)
    };
    let size = crate::reclaim::disk_usage(&from);
    if let Err(why) = clone_dir(ctx, s, &from, &tmp.join("target"), size) {
        return fail(why);
    }
    strip(&tmp.join("target"));
    let size = crate::reclaim::disk_usage(&tmp.join("target"));
    if let Err(e) = std::fs::write(tmp.join(MARKER), size.to_string()) {
        return fail(format!("{MARKER}: {e}"));
    }
    match std::fs::rename(&tmp, &entry) {
        Ok(()) => Save::Saved,
        // another job saved the key first
        Err(_) if entry.exists() => {
            let _ = std::fs::remove_dir_all(&tmp);
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

/// The entries of the cache (not the temporary ones), as they are on disk.
pub fn entries(dir: &Path) -> Vec<Entry> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.contains(".tmp-") || !e.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let path = e.path();
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

/// The keys to remove so that the rest fits in `max_bytes`: the least
/// recently used first. `keep` is never chosen.
pub fn lru_victims(entries: &[Entry], max_bytes: u64, keep: Option<&str>) -> Vec<String> {
    let mut total: u64 = entries.iter().map(|e| e.bytes).sum();
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

fn remove_entry(ctx: &ExecContext, s: &Settings, path: &Path) {
    if crate::git::move_to_trash(path, &s.trash).is_none() {
        crate::git::remove_now(path);
    }
    crate::git::empty_trash(ctx, &s.trash);
}

/// Remove the least recently used entries until the cache fits its cap, and
/// the temporary entries of jobs that died. Returns the number of entries
/// removed.
pub fn prune(ctx: &ExecContext, s: &Settings, keep: Option<&str>) -> usize {
    for e in std::fs::read_dir(&s.dir).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let old = e
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age > STALE_TMP);
        if name.contains(".tmp-") && old {
            remove_entry(ctx, s, &e.path());
        }
    }
    let victims = lru_victims(&entries(&s.dir), s.max_bytes, keep);
    for k in &victims {
        remove_entry(ctx, s, &s.dir.join(k));
    }
    victims.len()
}

/// Remove every entry (`local-ci cache prune --build`). Returns the number
/// removed.
pub fn clear(ctx: &ExecContext, s: &Settings) -> usize {
    let mut n = 0;
    for e in std::fs::read_dir(&s.dir).into_iter().flatten().flatten() {
        remove_entry(ctx, s, &e.path());
        n += 1;
    }
    n
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
    fn locks_are_found_below_the_root_but_not_in_target() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path();
        std::fs::write(p.join("Cargo.lock"), "a").unwrap();
        for d in ["sub", "target/debug", "node_modules/x", ".git"] {
            std::fs::create_dir_all(p.join(d)).unwrap();
        }
        std::fs::write(p.join("sub/Cargo.lock"), "b").unwrap();
        std::fs::write(p.join("target/debug/Cargo.lock"), "c").unwrap();
        std::fs::write(p.join("node_modules/x/Cargo.lock"), "d").unwrap();
        let locks = lock_digests(p);
        let names: Vec<&str> = locks.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["Cargo.lock", "sub/Cargo.lock"]);
        // the content is what counts
        std::fs::write(p.join("Cargo.lock"), "changed").unwrap();
        assert_ne!(locks[0].1, lock_digests(p)[0].1);
        assert!(applies(p) == p.join("Cargo.toml").is_file());
    }

    #[test]
    fn strip_rules() {
        assert!(should_strip("incremental", true));
        assert!(should_strip("mutants.out", true));
        assert!(should_strip("mutants.out.old", true));
        assert!(should_strip("default_1234.profraw", false));
        assert!(!should_strip("incremental", false));
        assert!(!should_strip("deps", true));
        assert!(!should_strip("libfoo.rlib", false));
        assert!(!should_strip("foo.profraw", true));
    }

    #[test]
    fn strip_removes_only_what_the_rules_name() {
        let t = tempfile::tempdir().unwrap();
        let d = t.path().join("target");
        for dir in [
            "debug/incremental/foo-1",
            "debug/deps",
            "x86_64-unknown-linux-gnu/debug/incremental",
            "llvm-cov-target/debug/deps",
            "mutants.out",
        ] {
            std::fs::create_dir_all(d.join(dir)).unwrap();
        }
        for f in [
            "debug/incremental/foo-1/a.o",
            "debug/deps/libfoo.rlib",
            "debug/deps/foo.d",
            "llvm-cov-target/debug/deps/default_1.profraw",
            "llvm-cov-target/debug/deps/libbar.rlib",
            "mutants.out/log",
        ] {
            std::fs::write(d.join(f), "x").unwrap();
        }
        strip(&d);
        assert!(!d.join("debug/incremental").exists());
        assert!(!d
            .join("x86_64-unknown-linux-gnu/debug/incremental")
            .exists());
        assert!(!d.join("mutants.out").exists());
        assert!(!d
            .join("llvm-cov-target/debug/deps/default_1.profraw")
            .exists());
        assert!(d.join("debug/deps/libfoo.rlib").exists());
        assert!(d.join("debug/deps/foo.d").exists());
        assert!(d.join("llvm-cov-target/debug/deps/libbar.rlib").exists());
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
        assert!(lru_victims(&es, 30, None).is_empty());
        assert_eq!(lru_victims(&es, 20, None), ["a"]);
        assert_eq!(lru_victims(&es, 10, None), ["a", "c"]);
        assert_eq!(lru_victims(&es, 0, None), ["a", "c", "b"]);
        // the entry just saved stays
        assert_eq!(lru_victims(&es, 20, Some("a")), ["c"]);
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
        }
    }

    #[test]
    fn a_saved_target_is_restored_without_what_was_stripped() {
        let t = tempfile::tempdir().unwrap();
        let s = settings(t.path());
        let (a, b) = (t.path().join("a"), t.path().join("b"));
        std::fs::create_dir_all(a.join("target/debug/incremental")).unwrap();
        std::fs::create_dir_all(a.join("target/debug/deps")).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        std::fs::write(a.join("target/debug/deps/lib.rlib"), "lib").unwrap();
        std::fs::write(a.join("target/debug/incremental/x"), "inc").unwrap();
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
        assert!(!b.join("target/debug/incremental").exists());
        // not over a target that is there
        assert!(matches!(restore(&c, &s, "k", &b), Restore::Skipped(_)));
        // no temporary entry is left
        let names: Vec<String> = std::fs::read_dir(&s.dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, ["k"]);
        let es = entries(&s.dir);
        assert_eq!(es.len(), 1);
        assert!(es[0].bytes > 0);
    }

    #[test]
    fn a_copy_that_is_not_a_clone_is_skipped_above_the_limit() {
        let t = tempfile::tempdir().unwrap();
        let mut s = settings(t.path());
        let a = t.path().join("a");
        std::fs::create_dir_all(a.join("target")).unwrap();
        std::fs::write(a.join("target/f"), vec![0u8; 8192]).unwrap();
        s.copy_limit = 0;
        let c = ctx();
        // on a file system that clones, this is saved; on one that does not, skipped
        // and nothing is left behind either way
        match save(&c, &s, "k", &a, 0) {
            Save::Saved => assert!(s.dir.join("k").is_dir()),
            Save::Skipped(_) => assert!(!s.dir.join("k").exists()),
            Save::Exists => unreachable!(),
        }
        let left: Vec<_> = std::fs::read_dir(&s.dir)
            .map(|d| d.flatten().map(|e| e.file_name()).collect())
            .unwrap_or_default();
        assert!(left.iter().all(|n| !n.to_string_lossy().contains(".tmp-")));
    }

    #[test]
    fn prune_removes_the_oldest_to_fit_and_the_dead_temporaries() {
        let t = tempfile::tempdir().unwrap();
        let mut s = settings(t.path());
        std::fs::create_dir_all(&s.dir).unwrap();
        for (k, age) in [("old", 100u64), ("new", 10)] {
            let p = s.dir.join(k);
            std::fs::create_dir_all(p.join("target")).unwrap();
            std::fs::write(p.join(MARKER), "1000").unwrap();
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(p.join(MARKER))
                .unwrap();
            f.set_modified(SystemTime::now() - Duration::from_secs(age))
                .unwrap();
        }
        let dead = s.dir.join("zz.tmp-1-0");
        std::fs::create_dir_all(&dead).unwrap();
        std::fs::File::open(&dead)
            .unwrap()
            .set_modified(SystemTime::now() - 2 * STALE_TMP)
            .unwrap();
        let live = s.dir.join("yy.tmp-2-0");
        std::fs::create_dir_all(&live).unwrap();
        s.max_bytes = 1500;
        let c = ctx();
        assert_eq!(prune(&c, &s, None), 1);
        assert!(!s.dir.join("old").exists());
        assert!(s.dir.join("new").exists());
        assert!(!dead.exists());
        assert!(live.exists());
        assert_eq!(clear(&c, &s), 2);
        assert!(entries(&s.dir).is_empty());
    }

    #[test]
    fn restoring_touches_the_entry() {
        let t = tempfile::tempdir().unwrap();
        let s = settings(t.path());
        let (a, b) = (t.path().join("a"), t.path().join("b"));
        std::fs::create_dir_all(a.join("target")).unwrap();
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
        assert!(Settings::from_ctx(&with("X", "1"), dir, true).is_none());
        assert!(Settings::from_ctx(&with("LOCAL_CI_NO_BUILD_CACHE", "1"), dir, false).is_none());
        assert!(Settings::from_ctx(&with("LOCAL_CI_NO_BUILD_CACHE", "0"), dir, false).is_some());
        let s =
            Settings::from_ctx(&with("LOCAL_CI_BUILD_CACHE_MAX_GB", "0.5"), dir, false).unwrap();
        assert_eq!(s.max_bytes, 512 * 1024 * 1024);
    }
}
