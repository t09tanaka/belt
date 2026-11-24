//! Running Git with an explicit environment (the process's own is never read).

use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;

pub(super) type Env = BTreeMap<String, String>;

fn run_with(env: &Env, cwd: &Path, extra: &[(&str, &OsStr)], args: &[&str]) -> Result<String> {
    let mut cmd = Command::new("git");
    cmd.args(["-c", "protocol.file.allow=always"])
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .envs(env)
        .env("GIT_TERMINAL_PROMPT", "0");
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let out = cmd.output().context("cannot run git")?;
    if !out.status.success() {
        bail!(
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim_end().to_string())
}

/// The oldest Git a worker runs with: a bundle is unpacked with
/// `git fetch --atomic` (all refs or none), which came with 2.31.
const MIN_GIT: (u32, u32) = (2, 31);

/// The major and minor number in what `git --version` prints (`git version
/// 2.39.5 (Apple Git-154)`, `git version 2.43.0.windows.1`, `git version
/// 2.50.0-rc1`). `None`: this is not what Git prints.
fn parse_version(text: &str) -> Option<(u32, u32)> {
    let word = text
        .strip_prefix("git version ")?
        .split_whitespace()
        .next()?;
    let mut parts = word.split('.');
    let major = parts.next()?.parse().ok()?;
    // (the minor number may be the last part, with a suffix: `50-rc1`)
    let minor = parts.next()?;
    let digits = minor.bytes().take_while(u8::is_ascii_digit).count();
    let minor = minor[..digits].parse().ok()?;
    Some((major, minor))
}

/// Is `found` a Git this worker can run with?
fn version_ok(found: (u32, u32)) -> bool {
    found >= MIN_GIT
}

/// Ask the Git that `env` finds for its version, and refuse one that is not
/// there, does not answer, or is older than [`MIN_GIT`].
pub(super) fn check_version(env: &Env) -> Result<()> {
    let out = Command::new("git")
        .arg("--version")
        .current_dir("/")
        .env_clear()
        .envs(env)
        .stdin(std::process::Stdio::null())
        .output()
        .context("cannot run git")?;
    let text = String::from_utf8_lossy(&out.stdout);
    let text = text.trim();
    if !out.status.success() {
        bail!(
            "git --version failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let Some(found) = parse_version(text) else {
        bail!("git --version printed {text:?}, which names no version");
    };
    if !version_ok(found) {
        bail!(
            "Git {}.{} is too old: {}.{} or newer is needed (git fetch --atomic)",
            found.0,
            found.1,
            MIN_GIT.0,
            MIN_GIT.1
        );
    }
    Ok(())
}

pub(super) fn run(env: &Env, cwd: &Path, args: &[&str]) -> Result<String> {
    run_with(env, cwd, &[], args)
}

/// Whether the command succeeds (its output is not wanted).
pub(super) fn ok(env: &Env, cwd: &Path, args: &[&str]) -> bool {
    run(env, cwd, args).is_ok()
}

/// Git inside the worktree `dir` and nowhere else: `GIT_DIR` and
/// `GIT_WORK_TREE` are set explicitly (so a `.git` that is missing or wrong is
/// an error, never a search upward into whatever repository holds the state
/// directory), and discovery may not climb above `ceiling` either.
pub(super) fn run_in_worktree(
    env: &Env,
    dir: &Path,
    ceiling: &Path,
    args: &[&str],
) -> Result<String> {
    let git_dir = dir.join(".git");
    run_with(
        env,
        dir,
        &[
            ("GIT_DIR", git_dir.as_os_str()),
            ("GIT_WORK_TREE", dir.as_os_str()),
            ("GIT_CEILING_DIRECTORIES", ceiling.as_os_str()),
        ],
        args,
    )
}

/// Git on the repository `git_dir` alone (`GIT_DIR` set, discovery not
/// above `ceiling`), with hooks off (`core.hooksPath=/dev/null` through
/// `GIT_CONFIG_*`, which a repository's own config cannot override) and
/// ended at `deadline` or when `stop` says so: its exit code, `None` when
/// `stop` ended it. Output is not read. No `GIT_*` variable of `env` reaches
/// it (review round 2: an inherited `GIT_OBJECT_DIRECTORY`,
/// `GIT_INDEX_FILE` or `GIT_CONFIG_PARAMETERS` would point it elsewhere).
/// It runs in a process group of its own, which is ended (SIGTERM, so git
/// removes its lockfiles, then SIGKILL after [`TERM_GRACE`]) on every way
/// out but its own exit: none of it outlives the call.
pub(super) fn run_bounded(
    env: &Env,
    git_dir: &Path,
    ceiling: &Path,
    args: &[&str],
    deadline: std::time::Instant,
    stop: &dyn Fn() -> bool,
) -> Result<Option<i32>> {
    use std::os::unix::process::CommandExt as _;
    let mut cmd = Command::new("git");
    cmd.args(args)
        .current_dir(git_dir)
        .env_clear()
        .envs(env.iter().filter(|(k, _)| !k.starts_with("GIT_")))
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_DIR", git_dir)
        .env("GIT_CEILING_DIRECTORIES", ceiling)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "core.hooksPath")
        .env("GIT_CONFIG_VALUE_0", "/dev/null")
        .process_group(0)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let spawned = cmd.spawn().context("cannot run git")?;
    // (decided now, while the leader is alive and unreaped: its group id
    // is its pid only if it is still the leader of the group it made)
    let pid = spawned.id();
    let own = crate::platform::process_group_of(pid) == i32::try_from(pid).ok();
    let mut child = Bounded(Some(spawned), own);
    // (review round 4, M-A: a git killed with SIGKILL may leave the lock
    // files of the refs it was writing; only step 4's own ref locks go, see
    // [`remove_git_locks`])
    let end = |child: &mut Bounded| {
        if child.end() {
            remove_git_locks(git_dir, args);
        }
    };
    loop {
        if let Some(status) = child.try_wait()? {
            return status
                .code()
                .map(Some)
                .with_context(|| format!("git {} was killed", args.join(" ")));
        }
        if stop() {
            end(&mut child);
            return Ok(None);
        }
        if std::time::Instant::now() >= deadline {
            end(&mut child);
            bail!("git {} did not end in time", args.join(" "));
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

/// How long a git that was asked to end (SIGTERM) has before SIGKILL.
pub(super) const TERM_GRACE: std::time::Duration = std::time::Duration::from_millis(500);

/// A git child of [`run_bounded`]: ended and reaped when dropped unreaped.
/// The flag: whether the child leads a process group of its own, decided
/// right after the spawn (review round 3, L-2).
struct Bounded(Option<std::process::Child>, bool);

impl Bounded {
    fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>> {
        let Some(child) = self.0.as_mut() else {
            bail!("git was already ended");
        };
        let status = child.try_wait()?;
        if status.is_some() {
            self.0 = None;
        }
        Ok(status)
    }

    /// SIGTERM to its group (only when the group is the child's own: it was
    /// spawned as the leader of a new one, and the leader, unreaped, keeps
    /// the group id from being reused), [`TERM_GRACE`] for it to exit, then
    /// SIGKILL to the group, and the reap. `true`: it had not exited in
    /// the grace, so SIGKILL ended it (its lock files may be left).
    fn end(&mut self) -> bool {
        let Some(mut child) = self.0.take() else {
            return false;
        };
        let pid = child.id();
        let own = self.1;
        let signal = |sig| {
            if own {
                crate::platform::signal_process_group(pid as i32, sig);
            } else {
                // (our own child: never pid <= 1)
                crate::platform::signal_pid(pid, sig);
            }
        };
        signal(libc::SIGTERM);
        let start = std::time::Instant::now();
        while start.elapsed() < TERM_GRACE && !exited(pid) {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let killed = !exited(pid);
        // (the leader may have exited, unreaped: what is left of its group
        // goes too)
        signal(libc::SIGKILL);
        let _ = child.wait();
        killed
    }
}

impl Drop for Bounded {
    fn drop(&mut self) {
        self.end();
    }
}

/// The ref directories whose `.lock` files a killed git of
/// [`run_bounded`] or [`remove_source_refs`] leaves to be removed: the refs
/// belt's receiving puts in a clone for a source
/// (`refs/local-ci/{snapshots,index}/<source>`).
const OWN_REF_DIRS: [&str; 2] = ["refs/local-ci/snapshots/", "refs/local-ci/index/"];

/// After a git of [`run_bounded`] was killed and reaped (review round 4,
/// M-A): the `<ref>.lock` it may have left for each argument naming a ref
/// of [`OWN_REF_DIRS`] (one plain name below it) goes, only when it is a
/// regular file below directories that are not links. The caller holds the
/// clone's import lock, and only holders of that lock write those refs:
/// step 4 (`update-ref -d` here), the bundle unpack of a publication
/// (`worker/blobs.rs` `unpack`) and the removal of the refs a refused or
/// discarded bundle made (`worker/blobs.rs` `drop_refs`). Other gits in a clone run without it (the
/// `checkout -f` of `hold_clone_at` and `settle`'s `branch -D` and
/// `worktree prune` under the clone's own lock, fast-worktree's add and
/// remove), so `packed-refs.lock` and `index.lock` may be a live git's and
/// are never removed: a stale `packed-refs.lock` makes step 4's git fail,
/// and the row is tried again an hour later (`ref_retry`).
fn remove_git_locks(git_dir: &Path, args: &[&str]) {
    let names = args.iter().filter(|a| {
        OWN_REF_DIRS.iter().any(|d| {
            a.strip_prefix(d).is_some_and(|leaf| {
                !leaf.is_empty() && leaf != "." && leaf != ".." && !leaf.contains('/')
            })
        })
    });
    for name in names {
        let name = format!("{name}.lock");
        let mut dir = git_dir.to_path_buf();
        let parts: Vec<&str> = name.split('/').collect();
        let (leaf, parents) = parts.split_last().expect("a name");
        let plain = parents.iter().all(|p| {
            dir.push(p);
            std::fs::symlink_metadata(&dir).is_ok_and(|m| m.is_dir())
        });
        let path = dir.join(leaf);
        if plain && std::fs::symlink_metadata(&path).is_ok_and(|m| m.is_file()) {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Step 4's removal of the refs belt's receiving put in the clone `dir`
/// for `source` (`refs/local-ci/{snapshots,index}/<source>`), each only
/// when it is there, with local-ci's hermetic, bounded git
/// (`remove_imported_refs_with`: no inherited `GIT_*`, no system or global
/// configuration, hooks off, discovery not above the clone's parent, each
/// git in a process group of its own, SIGTERM then SIGKILL, reaped) and
/// `deadline` for the call as a whole. `stop` is polled here while the call
/// runs on a scoped thread and is mapped onto the context's cancel handle
/// (`false`: `stop` ended it). The caller holds the clone's import lock and
/// has checked that `dir/.git` is a real directory (local-ci sets no
/// `GIT_DIR`: git finds that `.git` itself). After a call ended by `stop`
/// or the deadline, its git is reaped, so the `.lock` files of the two refs
/// it may have left are removed as [`remove_git_locks`] says (review round
/// 4, M-A: local-ci leaves them).
pub(super) fn remove_source_refs(
    env: &Env,
    dir: &Path,
    source: &str,
    deadline: std::time::Instant,
    stop: &dyn Fn() -> bool,
) -> Result<bool> {
    use local_ci::source::{remove_imported_refs_with, GitOptions};
    let ctx = local_ci::ExecContext::new(env.clone());
    let opts = GitOptions::new().hermetic().deadline(deadline);
    let joined = std::thread::scope(|s| {
        let call = s.spawn(|| remove_imported_refs_with(&ctx, dir, source, &opts));
        while !call.is_finished() {
            if !ctx.cancel.is_cancelled() && stop() {
                ctx.cancel.cancel();
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        call.join()
    });
    let Ok(result) = joined else {
        bail!("step 4's ref removal panicked");
    };
    let Err(e) = result else {
        return Ok(true);
    };
    let stopped = ctx.cancel.is_cancelled();
    if stopped || std::time::Instant::now() >= deadline {
        let refs = [
            format!("{}{source}", OWN_REF_DIRS[0]),
            format!("{}{source}", OWN_REF_DIRS[1]),
        ];
        let refs: Vec<&str> = refs.iter().map(String::as_str).collect();
        remove_git_locks(&dir.join(".git"), &refs);
    }
    if stopped {
        return Ok(false);
    }
    Err(e)
}

/// Whether our child `pid` has exited, leaving it unreaped.
fn exited(pid: u32) -> bool {
    // SAFETY: waitid on our own child; WNOWAIT leaves it unreaped.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let observed = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    // SAFETY: reads the field waitid filled in.
    observed != 0 || unsafe { info.si_pid() } == pid as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `git` on PATH that runs `script` (sh), for [`run_bounded`].
    fn fake_git(script: &str) -> (tempfile::TempDir, Env) {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        std::fs::create_dir(dir.path().join("repo")).unwrap();
        let git = bin.join("git");
        std::fs::write(&git, format!("#!/bin/sh\n{script}\n")).unwrap();
        let mut perm = std::fs::metadata(&git).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o755);
        std::fs::set_permissions(&git, perm).unwrap();
        let env: Env = [
            ("PATH".into(), format!("{}:/usr/bin:/bin", bin.display())),
            ("GIT_OBJECT_DIRECTORY".into(), "/elsewhere/objects".into()),
            (
                "GIT_CONFIG_PARAMETERS".into(),
                "'core.hooksPath'='/x'".into(),
            ),
            ("GIT_INDEX_FILE".into(), "/elsewhere/index".into()),
            ("GIT_WORK_TREE".into(), "/elsewhere".into()),
        ]
        .into_iter()
        .collect();
        (dir, env)
    }

    fn bounded(env: &Env, dir: &Path, within: u64, stop: &dyn Fn() -> bool) -> Result<Option<i32>> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(within);
        run_bounded(env, &dir.join("repo"), dir, &["show-ref"], deadline, stop)
    }

    /// Whether `pid` is still a live process (not gone, not a zombie).
    fn alive(pid: &str) -> bool {
        let out = Command::new("ps")
            .args(["-o", "stat=", "-p", pid.trim()])
            .output()
            .unwrap();
        let stat = String::from_utf8_lossy(&out.stdout);
        !stat.trim().is_empty() && !stat.trim().starts_with('Z')
    }

    /// Review round 2 (Env): no inherited `GIT_*` reaches step 4's git; its
    /// own are set.
    #[test]
    fn bounded_git_gets_no_inherited_git_variables() {
        let (dir, env) = fake_git(r#"env | grep '^GIT_' > "$GIT_DIR/../env"; exit 1"#);
        assert_eq!(
            bounded(&env, dir.path(), 10_000, &|| false).unwrap(),
            Some(1)
        );
        let seen = std::fs::read_to_string(dir.path().join("env")).unwrap();
        for gone in [
            "GIT_OBJECT_DIRECTORY",
            "GIT_CONFIG_PARAMETERS",
            "GIT_INDEX_FILE",
            "GIT_WORK_TREE",
        ] {
            assert!(!seen.contains(gone), "{gone} reached git: {seen}");
        }
        assert!(seen.contains("GIT_CONFIG_VALUE_0=/dev/null"), "{seen}");
        assert!(seen.contains("GIT_DIR="), "{seen}");
    }

    /// Review round 2 (M-b, L-b): `stop` ends a running git at once, with
    /// SIGTERM first (git removes its lockfile), and nothing of it is left.
    #[test]
    fn bounded_git_ends_on_stop_with_sigterm_first() {
        let (dir, env) = fake_git(
            r#"trap 'rm -f "$GIT_DIR/x.lock"; exit 143' TERM
: > "$GIT_DIR/x.lock"
sleep 30 &
echo $! > "$GIT_DIR/../pid"
wait"#,
        );
        let lock = dir.path().join("repo").join("x.lock");
        let pid = dir.path().join("pid");
        let start = std::time::Instant::now();
        let stop = || {
            pid.exists()
                && lock.exists()
                && std::fs::read_to_string(&pid).is_ok_and(|p| !p.trim().is_empty())
        };
        assert_eq!(bounded(&env, dir.path(), 20_000, &stop).unwrap(), None);
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "{:?}",
            start.elapsed()
        );
        assert!(!lock.exists(), "git's lockfile was left");
        assert!(
            !alive(&std::fs::read_to_string(&pid).unwrap()),
            "a child of git outlived it"
        );
    }

    /// Review round 2 (M-b): past its deadline a git that ignores SIGTERM is
    /// killed with its whole group, and the call is an error.
    #[test]
    fn bounded_git_is_killed_with_its_group_past_the_deadline() {
        let (dir, env) = fake_git(
            r#"trap '' TERM
sleep 30 &
echo $! > "$GIT_DIR/../pid"
wait"#,
        );
        let start = std::time::Instant::now();
        assert!(bounded(&env, dir.path(), 300, &|| false).is_err());
        let took = start.elapsed();
        assert!(took >= std::time::Duration::from_millis(300), "{took:?}");
        assert!(took < std::time::Duration::from_secs(5), "{took:?}");
        let pid = dir.path().join("pid");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !pid.exists() {
            assert!(std::time::Instant::now() < deadline, "no pid written");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            !alive(&std::fs::read_to_string(&pid).unwrap()),
            "a child of git outlived it"
        );
    }

    /// Review round 4 (M-A): a git that ignores SIGTERM is killed and
    /// reaped, and the `.lock` of the source ref it was deleting goes; the
    /// `packed-refs.lock` and `index.lock` another live git holds stay, as
    /// does the lock of a ref outside step 4's own.
    #[test]
    fn a_killed_git_leaves_only_its_own_ref_lock_removed() {
        let (dir, env) = fake_git(
            r#"trap '' TERM
mkdir -p "$GIT_DIR/refs/local-ci/snapshots" "$GIT_DIR/refs/a"
: > "$GIT_DIR/refs/local-ci/snapshots/src-1.lock"
: > "$GIT_DIR/refs/a/b.lock"
: > "$GIT_DIR/../ready"
while :; do sleep 1; done"#,
        );
        let repo = dir.path().join("repo");
        // (another git, alive, holds these)
        std::fs::write(repo.join("packed-refs.lock"), "").unwrap();
        std::fs::write(repo.join("index.lock"), "").unwrap();
        let ready = dir.path().join("ready");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let stop = || ready.exists();
        let ended = run_bounded(
            &env,
            &repo,
            dir.path(),
            &[
                "update-ref",
                "-d",
                "refs/local-ci/snapshots/src-1",
                "refs/a/b",
            ],
            deadline,
            &stop,
        )
        .unwrap();
        assert_eq!(ended, None);
        assert!(!repo.join("refs/local-ci/snapshots/src-1.lock").exists());
        assert!(repo.join("packed-refs.lock").exists());
        assert!(repo.join("index.lock").exists());
        assert!(repo.join("refs/a/b.lock").exists());
    }

    const SOURCE: &str = "src-0123456789abcdef0123456789abcdef";

    /// Real git in `dir` with the test process's PATH and nothing else.
    fn real_git(home: &Path, dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap())
            .env("HOME", home)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {out:?}");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// A repository at `dir` with both of [`SOURCE`]'s refs set.
    fn repo_with_refs(home: &Path, dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        real_git(home, dir, &["init", "-q"]);
        std::fs::write(dir.join("f"), "x").unwrap();
        let blob = real_git(home, dir, &["hash-object", "-w", "f"]);
        for d in OWN_REF_DIRS {
            real_git(home, dir, &["update-ref", &format!("{d}{SOURCE}"), &blob]);
        }
    }

    /// Step 4 through local-ci (review round 2, Env; round 1, M2): an
    /// inherited `GIT_DIR`, `GIT_INDEX_FILE` or `GIT_CONFIG_PARAMETERS`
    /// does not point its git at another repository, and the clone's
    /// `reference-transaction` hook does not run.
    #[test]
    fn step_4_removes_only_the_clones_refs_with_no_inherited_git_and_no_hook() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let clone = home.join("clones").join("r-0123456789abcdef");
        let other = home.join("other");
        repo_with_refs(home, &clone);
        repo_with_refs(home, &other);
        let hooks = clone.join(".git").join("hooks");
        std::fs::create_dir_all(&hooks).unwrap();
        let ran = home.join("hook-ran");
        let hook = hooks.join("reference-transaction");
        std::fs::write(&hook, format!("#!/bin/sh\ntouch '{}'\n", ran.display())).unwrap();
        let mut perm = std::fs::metadata(&hook).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o755);
        std::fs::set_permissions(&hook, perm).unwrap();
        let other_git = other.join(".git");
        let env: Env = [
            ("PATH".into(), std::env::var("PATH").unwrap()),
            ("HOME".into(), home.display().to_string()),
            ("GIT_DIR".into(), other_git.display().to_string()),
            ("GIT_INDEX_FILE".into(), "/elsewhere/index".into()),
            (
                "GIT_CONFIG_PARAMETERS".into(),
                "'core.hooksPath'='/x'".into(),
            ),
        ]
        .into_iter()
        .collect();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        assert!(remove_source_refs(&env, &clone, SOURCE, deadline, &|| false).unwrap());
        assert!(real_git(home, &clone, &["for-each-ref", "refs/local-ci/"]).is_empty());
        assert_eq!(
            real_git(home, &other, &["for-each-ref", "refs/local-ci/"])
                .lines()
                .count(),
            2
        );
        assert!(!ran.exists(), "a hook of the clone ran");
        // nothing there: no error
        assert!(remove_source_refs(&env, &clone, SOURCE, deadline, &|| false).unwrap());
    }

    /// A fake `git` for step 4: it ignores SIGTERM, leaves the lock of the
    /// snapshot ref (and of a ref that is not step 4's) in the repository
    /// named by `-C`, and runs until it is killed.
    fn stuck_git() -> (tempfile::TempDir, Env, std::path::PathBuf) {
        let (dir, env) = fake_git(
            r#"trap '' TERM
while [ $# -gt 0 ]; do [ "$1" = -C ] && D="$2"; shift; done
G="$D/.git"
mkdir -p "$G/refs/local-ci/snapshots" "$G/refs/a"
: > "$G/refs/local-ci/snapshots/src-0123456789abcdef0123456789abcdef.lock"
: > "$G/refs/a/b.lock"
sleep 30 &
echo $! > "$D/../pid"
: > "$D/../ready"
wait"#,
        );
        let repo = dir.path().join("repo");
        std::fs::create_dir(repo.join(".git")).unwrap();
        // (another git, alive, holds it)
        std::fs::write(repo.join(".git/packed-refs.lock"), "").unwrap();
        (dir, env, repo)
    }

    fn only_own_lock_removed(repo: &Path) {
        let g = repo.join(".git");
        assert!(!g
            .join(format!("refs/local-ci/snapshots/{SOURCE}.lock"))
            .exists());
        assert!(g.join("packed-refs.lock").exists());
        assert!(g.join("refs/a/b.lock").exists());
    }

    /// Review round 2 (M-b), round 4 (M-A): past its deadline step 4's git
    /// that ignores SIGTERM is killed with its group, the call is an error,
    /// and only the lock of step 4's own ref goes.
    #[test]
    fn step_4_past_its_deadline_is_an_error_and_removes_only_its_own_lock() {
        let (dir, env, repo) = stuck_git();
        let start = std::time::Instant::now();
        // (long enough for the fake git to have left its locks)
        let deadline = start + std::time::Duration::from_millis(1500);
        assert!(remove_source_refs(&env, &repo, SOURCE, deadline, &|| false).is_err());
        let took = start.elapsed();
        assert!(dir.path().join("ready").exists(), "the fake git never ran");
        assert!(took >= std::time::Duration::from_millis(1500), "{took:?}");
        assert!(took < std::time::Duration::from_secs(8), "{took:?}");
        only_own_lock_removed(&repo);
        assert!(
            !alive(&std::fs::read_to_string(dir.path().join("pid")).unwrap()),
            "a child of git outlived it"
        );
    }

    /// Review round 2 (M-b): `stop` ends step 4's git through the cancel
    /// handle: `false`, nothing of it left, only its own lock removed.
    #[test]
    fn step_4_ends_on_stop() {
        let (dir, env, repo) = stuck_git();
        let ready = dir.path().join("ready");
        let start = std::time::Instant::now();
        let deadline = start + std::time::Duration::from_secs(20);
        let stop = || ready.exists();
        assert!(!remove_source_refs(&env, &repo, SOURCE, deadline, &stop).unwrap());
        assert!(
            start.elapsed() < std::time::Duration::from_secs(8),
            "{:?}",
            start.elapsed()
        );
        only_own_lock_removed(&repo);
        assert!(
            !alive(&std::fs::read_to_string(dir.path().join("pid")).unwrap()),
            "a child of git outlived it"
        );
    }

    #[test]
    fn the_version_is_read_from_what_git_prints() {
        for (text, want) in [
            ("git version 2.31.0", Some((2, 31))),
            ("git version 2.39.5 (Apple Git-154)", Some((2, 39))),
            ("git version 2.43.0.windows.1", Some((2, 43))),
            ("git version 2.50.0-rc1", Some((2, 50))),
            ("git version 2.50-rc1", Some((2, 50))),
            ("git version 3.0", Some((3, 0))),
            ("git version 10.2.1\n", Some((10, 2))),
            ("git version 1.8.3.1", Some((1, 8))),
            ("", None),
            ("git version", None),
            ("git version ", None),
            ("git version 2", None),
            ("git version 2.", None),
            ("git version 2.x", None),
            ("git version .31", None),
            ("git version two.31", None),
            ("git version -2.31", None),
            ("git version 2.-31", None),
            ("version 2.31.0", None),
            ("Git version 2.31.0", None),
            ("git version2.31.0", None),
            ("hg version 2.31.0", None),
            ("usage: git [--version]", None),
            ("git version 99999999999.1", None),
            ("git version 2.99999999999", None),
        ] {
            assert_eq!(parse_version(text), want, "{text:?}");
        }
    }

    #[test]
    fn a_git_older_than_the_atomic_fetch_is_refused() {
        assert_eq!(MIN_GIT, (2, 31));
        for (found, ok) in [
            ((2, 31), true),
            ((2, 32), true),
            ((2, 56), true),
            ((3, 0), true),
            ((10, 0), true),
            ((2, 30), false),
            ((2, 0), false),
            ((1, 99), false),
            ((0, 0), false),
        ] {
            assert_eq!(version_ok(found), ok, "{found:?}");
        }
    }
}
