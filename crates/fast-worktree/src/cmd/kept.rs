//! `refs/fast-worktree/kept/`: the commits `hook remove` keeps, and `kept
//! ls` / `kept prune`.
//!
//! One removal keeps its refs in a group of its own,
//! `refs/fast-worktree/kept/<name>-<YYYYMMDD>-<HHMMSS>` (UTC; `-2`, `-3`...
//! when that name is taken): `<group>/head` for the worktree's `HEAD`, and
//! `<group>/sub/<path>` for each checked-out submodule's. `prune` reads the
//! date from the group's name.

use super::cache::table;
use crate::error::Result;
use crate::git;
use crate::util::{errln, out, outln};
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub const KEPT_REFS: &str = "refs/fast-worktree/kept/";

/// How long `kept prune` keeps refs by default.
pub const PRUNE_DAYS: u64 = 30;

/// A kept ref younger than this is never deleted for being held by a
/// branch. The removal that made it may not be done yet: it makes the ref
/// first, and deletes the worktree's own branch (which holds the same
/// commit until then) after. And while the removed worktree's files are
/// kept in the trash (24 hours, and until the first sweep after that, an
/// hour later at most), the command that the removal printed to restore
/// them names the ref.
pub const GRACE_SECS: u64 = 25 * 3_600;

/// The day (UTC) on which a ref kept now is deleted when nothing else
/// holds its commit, for the line that says so when it is made.
pub fn expiry_date() -> String {
    let at = UNIX_EPOCH + std::time::Duration::from_secs(now_secs() + PRUNE_DAYS * 86_400);
    crate::cache::date(at)
}

/// The refs one removal keeps, under a group that is made when the first
/// ref is added.
pub struct Group<'a> {
    main: &'a Path,
    name: String,
    base: Option<String>,
}

impl<'a> Group<'a> {
    /// A group for the worktree `name` of the repository at `main`.
    pub fn new(main: &'a Path, name: &str) -> Group<'a> {
        Group {
            main,
            name: name.to_string(),
            base: None,
        }
    }

    /// The group's ref prefix, chosen the first time: a name no ref uses.
    fn base(&mut self) -> Option<String> {
        if let Some(b) = &self.base {
            return Some(b.clone());
        }
        let time = compact_now();
        let mut stem = format!("{KEPT_REFS}{}-{time}", self.name);
        if !git::succeeds(self.main, &["check-ref-format", &format!("{stem}/head")]) {
            stem = format!("{KEPT_REFS}worktree-{time}");
        }
        for n in 1..10 {
            let b = if n == 1 {
                stem.clone()
            } else {
                format!("{stem}-{n}")
            };
            // A pattern matches the ref itself and every ref below it.
            let used = git::output(self.main, &["for-each-ref", "--format=%(refname)", &b])
                .map_or(true, |s| !s.trim().is_empty());
            if !used {
                self.base = Some(b.clone());
                return Some(b);
            }
        }
        None
    }

    /// Keep `sha` as `<group>/<leaf>`. Returns the ref.
    pub fn keep(&mut self, leaf: &str, sha: &str) -> std::result::Result<String, String> {
        let r = format!("{}/{leaf}", self.base().ok_or("no free ref name")?);
        if !git::succeeds(self.main, &["check-ref-format", &r]) {
            return Err(format!("{r} is not a valid ref name"));
        }
        // The empty old value: only when the ref does not exist yet.
        git::output(self.main, &["update-ref", &r, sha, ""])
            .map(|_| r)
            .map_err(|e| e.to_string())
    }

    /// Keep the commit `sha` of the repository whose git directory is
    /// `git_dir` (a submodule) as `<group>/<leaf>`, fetching it into the
    /// main repository first: the submodule's own git directory may go.
    pub fn fetch(
        &mut self,
        leaf: &str,
        git_dir: &Path,
        sha: &str,
    ) -> std::result::Result<String, String> {
        let r = format!("{}/{leaf}", self.base().ok_or("no free ref name")?);
        if !git::succeeds(self.main, &["check-ref-format", &r]) {
            return Err(format!("{r} is not a valid ref name"));
        }
        let src = git_dir.to_string_lossy();
        let fetch = |what: &str| {
            git::output(
                self.main,
                &[
                    "fetch",
                    "-q",
                    "--no-tags",
                    "--no-recurse-submodules",
                    "--no-write-fetch-head",
                    &src,
                    &format!("{what}:{r}"),
                ],
            )
        };
        // By commit, else (an old git that does not fetch commits by name)
        // its HEAD, which is that commit.
        let fetched = fetch(sha).or_else(|_| fetch("HEAD"));
        let got = git::output(self.main, &["rev-parse", "--verify", "-q", &r])
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        match fetched {
            Ok(_) if got == sha => Ok(r),
            Ok(_) => {
                let _ = git::output(self.main, &["update-ref", "-d", &r]);
                Err(format!("fetched {got} instead of {sha}"))
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

/// `YYYYMMDD-HHMMSS` (UTC), for a ref name.
pub fn compact_now() -> String {
    compact(now_secs())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn compact(secs: u64) -> String {
    let t = UNIX_EPOCH + std::time::Duration::from_secs(secs);
    let s = secs % 86_400;
    format!(
        "{}-{:02}{:02}{:02}",
        crate::cache::date(t).replace('-', ""),
        s / 3_600,
        s % 3_600 / 60,
        s % 60
    )
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant's
/// days_from_civil).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The time in a kept ref's name: the rightmost `-YYYYMMDD-HHMMSS` of the
/// part after `kept/` up to the next `/`, followed by nothing or by `-<n>`.
/// (`<name>` may hold digits and dashes of its own.)
pub fn kept_time(refname: &str) -> Option<u64> {
    let group = refname.strip_prefix(KEPT_REFS)?.split('/').next()?;
    let b = group.as_bytes();
    let digits = |r: std::ops::Range<usize>| b[r].iter().all(u8::is_ascii_digit);
    (0..b.len()).rev().find_map(|i| {
        if b.len() < i + 16 || b[i] != b'-' || b[i + 9] != b'-' {
            return None;
        }
        if !digits(i + 1..i + 9) || !digits(i + 10..i + 16) {
            return None;
        }
        let rest = &group[i + 16..];
        let suffix_ok = rest.is_empty()
            || rest
                .strip_prefix('-')
                .is_some_and(|n| !n.is_empty() && n.bytes().all(|c| c.is_ascii_digit()));
        if !suffix_ok {
            return None;
        }
        let num = |r: std::ops::Range<usize>| group[r].parse::<i64>().ok();
        let (y, mo, d) = (num(i + 1..i + 5)?, num(i + 5..i + 7)?, num(i + 7..i + 9)?);
        let (h, mi, s) = (
            num(i + 10..i + 12)?,
            num(i + 12..i + 14)?,
            num(i + 14..i + 16)?,
        );
        if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || s > 60 {
            return None;
        }
        let secs = days_from_civil(y, mo, d) * 86_400 + h * 3_600 + mi * 60 + s;
        u64::try_from(secs).ok()
    })
}

/// The kept refs of the repository at `dir`, with their commits.
pub fn kept_refs(dir: &Path) -> Result<Vec<(String, String)>> {
    let out = git::output(
        dir,
        &[
            "for-each-ref",
            "--format=%(refname) %(objectname)",
            KEPT_REFS,
        ],
    )?;
    Ok(out
        .lines()
        .filter_map(|l| l.split_once(' '))
        .map(|(r, o)| (r.to_string(), o.to_string()))
        .collect())
}

fn count(n: usize) -> String {
    format!("{n} {}", if n == 1 { "ref" } else { "refs" })
}

/// `kept ls`.
pub fn ls() -> Result<i32> {
    let refs = kept_refs(Path::new("."))?;
    if refs.is_empty() {
        outln!("no refs under {KEPT_REFS}");
        return Ok(0);
    }
    let now = now_secs();
    let rows: Vec<[String; 3]> = refs
        .iter()
        .map(|(r, o)| {
            let when = match kept_time(r) {
                Some(t) => {
                    let st = UNIX_EPOCH + std::time::Duration::from_secs(t);
                    format!(
                        "{} ({}d ago)",
                        crate::cache::datetime(st),
                        now.saturating_sub(t) / 86_400
                    )
                }
                None => "?".to_string(),
            };
            [r.clone(), o[..o.len().min(12)].to_string(), when]
        })
        .collect();
    out!("{}", table(&["REF", "COMMIT", "KEPT (UTC)"], &rows));
    outln!(
        "{}; `git push --mirror` pushes them too. Each is deleted by itself {PRUNE_DAYS} days after it was kept, sooner when a branch or tag has its commit (`fast-worktree kept prune` does it at once)",
        count(refs.len())
    );
    Ok(0)
}

/// `kept prune [--older-than <days>]`: delete the kept refs whose name says
/// they are older than that, and those whose commit a branch or a tag
/// holds ([`prune_refs`]). Refs whose name has no time are left alone.
pub fn prune(older_than_days: Option<u64>) -> Result<i32> {
    let days = older_than_days.unwrap_or(PRUNE_DAYS);
    let done = prune_refs(Path::new("."), days)?;
    let mut held = 0;
    for d in &done.deleted {
        match &d.reason {
            Reason::HeldBy(holder) => {
                held += 1;
                outln!("pruned {} (its commit is held by {holder})", d.name);
            }
            Reason::Expired => outln!("pruned {}", d.name),
        }
    }
    outln!(
        "pruned {} kept for {days} days or more, and {} whose commit a branch or tag has",
        count(done.deleted.len() - held),
        count(held)
    );
    if done.failed > 0 {
        errln!(
            "fast-worktree: {} could not be deleted (one of them changed meanwhile); run it again",
            count(done.failed)
        );
    }
    Ok(i32::from(done.failed > 0))
}

/// Delete the kept refs of the repository at `dir` that are `days` days
/// old or older, whatever holds their commits, and nothing else: what
/// [`crate::Repository::prune_kept_refs`] has done since it was released.
/// One git for each ref, each deleted only if it still has the commit that
/// was read. Returns how many were deleted and how many could not be.
pub fn prune_by_age(dir: &Path, days: u64) -> Result<(usize, usize)> {
    let now = now_secs();
    let mut pruned = 0;
    let mut failed = 0;
    for (r, o) in kept_refs(dir)? {
        let Some(t) = kept_time(&r) else {
            continue;
        };
        if now.saturating_sub(t) < days.saturating_mul(86_400) {
            continue;
        }
        match git::output(dir, &["update-ref", "-d", &r, &o]) {
            Ok(_) => {
                outln!("pruned {r}");
                pruned += 1;
            }
            Err(e) => {
                errln!("fast-worktree: {e}");
                failed += 1;
            }
        }
    }
    Ok((pruned, failed))
}

/// Why a kept ref was deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    /// A local branch or a tag holds its commit: the ref protected
    /// nothing. The name is the branch (`main`) or the tag (`tags/v1`).
    HeldBy(String),
    /// It was kept for the number of days asked for ([`PRUNE_DAYS`] when
    /// nobody asked). Unless something else holds its commit, `git gc` may
    /// drop it now.
    Expired,
}

/// A kept ref that was deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deleted {
    pub name: String,
    pub commit: String,
    pub reason: Reason,
}

/// What [`prune_refs`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Pruned {
    pub deleted: Vec<Deleted>,
    /// How many refs should have been deleted and were not.
    pub failed: usize,
}

/// What to do with the kept refs `refs` (name and commit) at time `now`.
/// Returns the indexes of those that are `days` days old or older, which
/// are deleted whatever holds their commit, and of those to ask git about:
/// a worktree's `HEAD` (`<group>/head`) at least [`GRACE_SECS`] old, which
/// is deleted when a branch holds its commit. A submodule's `HEAD`
/// (`<group>/sub/...`) is a commit of another repository, which no branch
/// of this one holds; a ref whose name has no time is left alone.
fn plan(refs: &[(String, String)], now: u64, days: u64) -> (Vec<usize>, Vec<usize>) {
    let (mut expired, mut ask) = (Vec::new(), Vec::new());
    for (i, (r, _)) in refs.iter().enumerate() {
        let Some(t) = kept_time(r) else {
            continue;
        };
        let age = now.saturating_sub(t);
        if age >= days.saturating_mul(86_400) {
            expired.push(i);
        } else if age >= GRACE_SECS && r.ends_with("/head") && !r.contains("/sub/") {
            ask.push(i);
        }
    }
    (expired, ask)
}

/// What `git name-rev` printed, as commit to holder: each line is a commit
/// and how to reach it from a ref (`main~3`, `tags/v1^0`), or `undefined`
/// when none of the refs asked about has it. The holder is the ref's part
/// of that. A commit git said nothing about is not in the result.
fn holders(name_rev: &str) -> HashMap<String, String> {
    name_rev
        .lines()
        .filter_map(|l| l.trim().split_once(' '))
        .filter(|(_, name)| *name != "undefined")
        .filter_map(|(commit, name)| {
            let holder = name.split(['~', '^']).next().filter(|h| !h.is_empty())?;
            Some((commit.to_string(), holder.to_string()))
        })
        .collect()
}

/// Which of `commits` a local branch or a tag of the repository at `dir`
/// holds, and which one, with one git process for all of them (`git
/// name-rev --refs=refs/heads/* --refs=refs/tags/*`). When git fails, or
/// says nothing about a commit (an object that is gone), it does not count
/// as held.
///
/// Only what the user alone deletes counts. A remote-tracking branch does
/// not: it goes when somebody else deletes or rewrites the branch and the
/// next fetch prunes it, and a commit that only it held is then held by
/// nothing, with its kept ref long deleted. A worktree's `HEAD` holds
/// nothing either: a worktree goes with one `git worktree remove`, and one
/// that a removal moved into the trash is still registered until the trash
/// entry is deleted.
fn held_by(dir: &Path, commits: &[&str]) -> HashMap<String, String> {
    let mut held = HashMap::new();
    // Far below what a command line may hold, whatever the hash length.
    for group in commits.chunks(500) {
        let mut args = vec!["name-rev", "--refs=refs/heads/*", "--refs=refs/tags/*"];
        args.extend_from_slice(group);
        if let Ok(out) = git::output(dir, &args) {
            held.extend(holders(&out));
        }
    }
    held
}

/// Delete the kept refs of the repository at `dir` that are no longer
/// needed, in this order:
///
/// 1. A worktree's `HEAD` whose commit a local branch or a tag holds
///    protects nothing, and is deleted without waiting for its age, once
///    it is [`GRACE_SECS`] old.
/// 2. Any other may be the only thing that holds its commit, and is
///    deleted once it is `days` days old.
///
/// The automatic sweep and `kept prune` both call this. All refs are
/// deleted by one `git update-ref --stdin`, each only if it still has the
/// commit that was read. When that fails (one ref changed meanwhile), none
/// is deleted and all are counted as failed: they are still there for the
/// next time, which reads them afresh.
pub fn prune_refs(dir: &Path, days: u64) -> Result<Pruned> {
    let refs = kept_refs(dir)?;
    let (expired, ask) = plan(&refs, now_secs(), days);
    let commits: Vec<&str> = ask.iter().map(|&i| refs[i].1.as_str()).collect();
    let held = if commits.is_empty() {
        HashMap::new()
    } else {
        held_by(dir, &commits)
    };
    let of = |i: usize, reason: Reason| Deleted {
        name: refs[i].0.clone(),
        commit: refs[i].1.clone(),
        reason,
    };
    let mut deleted: Vec<Deleted> = ask
        .into_iter()
        .filter_map(|i| Some(of(i, Reason::HeldBy(held.get(&refs[i].1)?.clone()))))
        .collect();
    deleted.extend(expired.into_iter().map(|i| of(i, Reason::Expired)));
    if deleted.is_empty() {
        return Ok(Pruned::default());
    }
    let input: String = deleted
        .iter()
        .map(|d| format!("delete {} {}\n", d.name, d.commit))
        .collect();
    if git::output_with_stdin(dir, &["update-ref", "--stdin"], &input).is_err() {
        return Ok(Pruned {
            deleted: Vec::new(),
            failed: deleted.len(),
        });
    }
    Ok(Pruned { deleted, failed: 0 })
}

thread_local! {
    /// What [`prune_auto`] deleted in this thread since [`take_deleted`].
    static DELETED: RefCell<Vec<Deleted>> = const { RefCell::new(Vec::new()) };
}

/// The kept refs that the automatic sweeps of this thread deleted since
/// the last call: for the result of the library call that made them.
pub fn take_deleted() -> Vec<Deleted> {
    DELETED.with(|d| d.take())
}

/// The line that says why the kept ref `d` was deleted by itself.
fn why(d: &Deleted) -> String {
    match &d.reason {
        Reason::HeldBy(holder) => format!(
            "fast-worktree: deleted {}: its commit {} is held by {holder}",
            d.name, d.commit
        ),
        Reason::Expired => format!(
            "fast-worktree: deleted {}: {PRUNE_DAYS} days passed since it was kept; unless a \
             branch or a tag has its commit {}, `git gc` may drop it now",
            d.name, d.commit
        ),
    }
}

/// [`prune_refs`] for the sweep that every `add`, `remove` and library
/// call makes for its repository (never `pool fill`, which mostly runs in
/// the background). No ref is deleted in silence: one line for each goes
/// to stderr, with why, and the list is kept for [`take_deleted`]. Returns
/// what was deleted.
pub fn prune_auto(dir: &Path) -> Vec<Deleted> {
    let Ok(done) = prune_refs(dir, PRUNE_DAYS) else {
        return Vec::new();
    };
    for d in &done.deleted {
        errln!("{}", why(d));
    }
    if done.failed > 0 {
        errln!(
            "fast-worktree: {} that are no longer needed could not be deleted; a later command \
             tries again",
            count(done.failed)
        );
    }
    DELETED.with(|d| d.borrow_mut().extend(done.deleted.iter().cloned()));
    done.deleted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_time_is_read_from_the_right_of_the_group_name() {
        let t = kept_time("refs/fast-worktree/kept/a-20261026-120000/head").unwrap();
        assert_eq!(compact(t), "20261026-120000");
        let t2 = kept_time("refs/fast-worktree/kept/a-20261026-120000-3/sub/x/y").unwrap();
        assert_eq!(t2, t);
        // A name with a date of its own.
        let t3 = kept_time("refs/fast-worktree/kept/fix-20200101-000000-20261026-120000").unwrap();
        assert_eq!(t3, t);
        assert_eq!(kept_time("refs/fast-worktree/kept/plain/head"), None);
        assert_eq!(
            kept_time("refs/fast-worktree/kept/a-20261026-120000x"),
            None
        );
        assert_eq!(kept_time("refs/fast-worktree/kept/a-20261326-120000"), None);
        assert_eq!(kept_time("refs/heads/a-20261026-120000"), None);
        assert_eq!(
            kept_time("refs/fast-worktree/kept/x-19700101-000001"),
            Some(1)
        );
    }

    #[test]
    fn a_ref_expires_at_exactly_the_limit_and_is_asked_about_after_the_grace() {
        let now = 1_800_000_000;
        let day = 86_400;
        let name = |ago: u64, leaf: &str| format!("{KEPT_REFS}wt-{}/{leaf}", compact(now - ago));
        let refs: Vec<(String, String)> = [
            name(30 * day, "head"),           // 0: exactly 30 days
            name(30 * day - 1, "head"),       // 1: a second short of it
            name(GRACE_SECS, "head"),         // 2: exactly the grace
            name(GRACE_SECS - 1, "head"),     // 3: a removal may be in flight
            name(2 * day, "sub/lib/head"),    // 4: a submodule's commit
            name(31 * day, "sub/lib"),        // 5: old, whatever it is
            format!("{KEPT_REFS}plain/head"), // 6: no time in its name
        ]
        .into_iter()
        .map(|r| (r, "c".repeat(40)))
        .collect();
        assert_eq!(plan(&refs, now, 30), (vec![0, 5], vec![1, 2]));
        // `--older-than 0`: everything with a time, nothing to ask.
        assert_eq!(plan(&refs, now, 0), (vec![0, 1, 2, 3, 4, 5], vec![]));
    }

    #[test]
    fn the_holder_is_read_from_what_name_rev_prints() {
        let out = "aaaa main~3\nbbbb tags/v1^0\ncccc undefined\ndddd feature/nested/x\n\neeee\n";
        let h = holders(out);
        assert_eq!(h.get("aaaa").map(String::as_str), Some("main"));
        assert_eq!(h.get("bbbb").map(String::as_str), Some("tags/v1"));
        assert_eq!(h.get("dddd").map(String::as_str), Some("feature/nested/x"));
        // Held by nothing, and a line that is no answer.
        assert_eq!(h.len(), 3, "{h:?}");
    }

    #[test]
    fn every_deletion_says_why() {
        let d = |reason| Deleted {
            name: format!("{KEPT_REFS}a-20261026-120000/head"),
            commit: "c".repeat(40),
            reason,
        };
        let held = why(&d(Reason::HeldBy("main".into())));
        assert!(
            held.contains("is held by main") && held.contains(&"c".repeat(40)),
            "{held}"
        );
        assert!(!held.contains("git gc"), "{held}");
        let expired = why(&d(Reason::Expired));
        assert!(
            expired.contains("30 days passed") && expired.contains("git gc"),
            "{expired}"
        );
        assert!(expired.contains(&format!("{KEPT_REFS}a-20261026-120000/head")));
    }

    #[test]
    fn compact_times_round_trip() {
        for secs in [0, 951_782_400, 1_793_016_085, 4_102_444_799] {
            let c = compact(secs);
            let r = format!("{KEPT_REFS}n-{c}");
            assert_eq!(kept_time(&r), Some(secs), "{c}");
        }
        let now = compact_now();
        assert_eq!(now.len(), "20261026-120000".len(), "{now}");
    }
}
