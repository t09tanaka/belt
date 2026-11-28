//! Repository detection for the command key: the main worktree path, so
//! every linked worktree collapses to one key.

use std::path::{Path, PathBuf};
use std::process::Command;

/// `cwd` relative to the worktree root ("" at the root, "api/" below it),
/// or "" outside git. Lets subprojects of one repository learn separately.
pub fn subdir(cwd: &Path) -> String {
    Command::new("git")
        .args(["rev-parse", "--show-prefix"])
        .current_dir(cwd)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .unwrap_or_default()
}

/// The worktree a command is queued from: its root and what is checked out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Worktree {
    /// `git rev-parse --show-toplevel`.
    pub path: String,
    /// The branch, or the short commit hash when HEAD is detached.
    pub branch: String,
}

fn git_line(cwd: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .ok()
        .filter(|o| o.status.success())?;
    let s = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!s.is_empty()).then_some(s)
}

/// The worktree `cwd` belongs to, or `None` outside git (or in a bare
/// repository, which has no worktree).
pub fn worktree(cwd: &Path) -> Option<Worktree> {
    let path = git_line(cwd, &["rev-parse", "--show-toplevel"])?;
    let branch = git_line(cwd, &["symbolic-ref", "--short", "-q", "HEAD"])
        .or_else(|| git_line(cwd, &["rev-parse", "--short", "HEAD"]))
        .unwrap_or_default();
    Some(Worktree { path, branch })
}

pub fn main_worktree(cwd: &Path) -> Option<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--path-format=absolute", "--git-common-dir"])
        .current_dir(cwd)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let common = String::from_utf8(out.stdout).ok()?;
    let common = PathBuf::from(common.trim());
    // `<main>/.git` for a normal repo; a bare repo's common dir *is* the
    // repo, in which case we keep it as-is.
    let main = if common.file_name().map(|n| n == ".git").unwrap_or(false) {
        common.parent().map(Path::to_path_buf)?
    } else {
        common
    };
    Some(main.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(dir: &Path, args: &[&str]) {
        let ok = Command::new("git")
            .args(["-c", "user.name=t", "-c", "user.email=t@example.com"])
            .args(args)
            .current_dir(dir)
            .status()
            .unwrap()
            .success();
        assert!(ok, "git {args:?}");
    }

    #[test]
    fn a_linked_worktree_reports_its_own_root_and_branch() {
        let d = tempfile::tempdir().unwrap();
        let main = d.path().join("main");
        std::fs::create_dir(&main).unwrap();
        git(&main, &["init", "-q", "-b", "trunk"]);
        git(&main, &["commit", "-q", "--allow-empty", "-m", "init"]);
        let linked = d.path().join("linked");
        git(
            &main,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "feat/x",
                linked.to_str().unwrap(),
            ],
        );
        let sub = linked.join("sub");
        std::fs::create_dir(&sub).unwrap();

        let m = worktree(&main).unwrap();
        assert_eq!(m.branch, "trunk");
        let l = worktree(&sub).unwrap();
        assert_eq!(l.branch, "feat/x");
        assert!(l.path.ends_with("linked"), "{}", l.path);
        assert_ne!(m.path, l.path);
        // Both collapse to one repository key.
        assert_eq!(main_worktree(&sub), main_worktree(&main));

        git(&linked, &["checkout", "-q", "--detach"]);
        let det = worktree(&linked).unwrap();
        assert!(!det.branch.is_empty() && det.branch != "feat/x");
        assert!(det.branch.len() >= 4 && det.branch.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
