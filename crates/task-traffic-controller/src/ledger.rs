//! Ledger schema and load/save (spec "Ledger and locking").
//!
//! Callers must hold the ledger lock (`lock::ledger`) around load+save.

use crate::admit::{QueueEntry, State};
use crate::error::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;

pub const SCHEMA_VERSION: u32 = 1;
/// The version of a ledger that holds managed entries (see [`Ledger`]).
pub const MANAGED_SCHEMA_VERSION: u32 = 2;
/// The version of a ledger that holds nested entries (see [`Ledger`]).
pub const NESTED_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Entry {
    pub id: String,
    pub pid: u32,
    pub state: State,
    pub front: bool,
    pub weight: u32,
    pub predicted_rss: u64,
    /// Learned wall time, for estimates. None when unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_secs: Option<f64>,
    pub repo: String,
    #[serde(default)]
    pub subdir: String,
    pub argv: String,
    pub key: String,
    pub seq: u64,
    pub enqueued_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// Cores the running command's process tree was last measured using,
    /// written by the running `task-traffic-controller` itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub using: Option<f64>,
    /// Started ahead of a waiting entry that did not fit the budget.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub backfilled: bool,
    /// A human-readable name for the run (`exec --label`), shown by `status`
    /// and `top` instead of the command line. Never part of the history key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The git worktree (`git rev-parse --show-toplevel`) the command was
    /// queued from, so `top` can tell apart runs of one repository started
    /// from different worktrees. Absent outside git and in entries written
    /// before it existed; never part of the history key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<String>,
    /// The branch checked out in that worktree when it was queued, or the
    /// short commit hash when HEAD was detached.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// The running entry whose command started this one (a nested `exec`).
    /// Such an entry is admitted against the same budget, and what it takes
    /// is counted inside its parent's share first (see
    /// [`crate::admit::used_budget`]). Absent for a top-level command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// CPU seconds of the nested entries of this one that have finished.
    /// The entry's own measurement (`rusage` of its command) covers them
    /// too; this is taken off before its run is recorded, so it does not
    /// learn the weight of what it only started.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub nested_cpu_secs: f64,
    /// Fields this binary does not know, kept so that rewriting the ledger
    /// does not drop what a newer binary wrote.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

fn is_zero(v: &f64) -> bool {
    *v == 0.0
}

/// The ledger. In memory (and in `status --json`) it is always `entries`;
/// the file has two shapes, chosen by [`Ledger::save`].
///
/// A ledger without managed entries is written exactly as before
/// (`version` 1, `entries`), so older binaries keep reading it. A ledger
/// that holds a managed entry (one with a sidecar in the `managed` directory
/// beside the file) is written with `version` 2 and its entries under
/// `entries_v2`, with no `entries` key at all. An older binary then fails
/// to parse it (a required field is missing) and so neither reaps nor
/// rewrites it: it sees only plain entries and does not know that managed
/// ones keep their budget after the owner's lease is gone, so failing
/// closed is the only safe answer.
///
/// A ledger that holds a nested entry (one with a `parent`, or one that
/// has counted the CPU time of nested entries) is written with `version`
/// 3 and its entries under `entries_v3` alone, for the same reason: a
/// binary from before nested admission would rewrite it without those
/// fields, which charges a nested command twice and leaves a waiting one
/// queued behind its own parent. Such a binary fails on the file until
/// the nested commands have finished.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ledger {
    pub version: u32,
    pub next_seq: u64,
    pub entries: Vec<Entry>,
    /// Top-level fields this binary does not know, kept on rewrite.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// The file's shapes. Borrowed to write, owned to read.
#[derive(Serialize, Deserialize)]
struct Wire<E> {
    version: u32,
    next_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entries: Option<E>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entries_v2: Option<E>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entries_v3: Option<E>,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

impl Default for Ledger {
    fn default() -> Self {
        Ledger {
            version: SCHEMA_VERSION,
            next_seq: 1,
            entries: Vec::new(),
            extra: Default::default(),
        }
    }
}

impl Ledger {
    pub fn load(path: &Path) -> Result<Ledger> {
        match std::fs::read(path) {
            Ok(bytes) if bytes.is_empty() => Ok(Ledger::default()),
            Ok(bytes) => {
                let w: Wire<Vec<Entry>> = serde_json::from_slice(&bytes)?;
                let mut shapes = [w.entries, w.entries_v2, w.entries_v3]
                    .into_iter()
                    .flatten();
                let (Some(entries), None) = (shapes.next(), shapes.next()) else {
                    return Err(Error::Ledger(
                        "a ledger has exactly one of `entries`, `entries_v2` and `entries_v3`"
                            .into(),
                    ));
                };
                Ok(Ledger {
                    version: w.version,
                    next_seq: w.next_seq,
                    entries,
                    extra: w.extra,
                })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Ledger::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// The format version the ledger file on disk is written in: 1 (readable
    /// by older binaries), 2 (holds a managed entry) or 3 (holds a nested
    /// entry). A missing or
    /// unreadable file is the plain format, which is what the next save
    /// writes when no managed entry exists.
    pub fn file_format(path: &Path) -> u32 {
        std::fs::read(path)
            .ok()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
            .and_then(|v| v.get("version")?.as_u64())
            .map_or(SCHEMA_VERSION, |v| v as u32)
    }

    /// Atomic write: temp file in the same directory, then rename.
    pub fn save(&self, path: &Path) -> Result<()> {
        let dir = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(dir)?;
        let managed_dir = dir.join("managed");
        // Fail closed: a sidecar that cannot be looked for (a permission or
        // I/O error) counts as there, so an older binary is never handed a
        // v1 ledger that hides a managed entry.
        let managed = self.entries.iter().any(|e| {
            managed_dir
                .join(format!("{}.json", e.id))
                .try_exists()
                .unwrap_or(true)
        });
        let nested = self
            .entries
            .iter()
            .any(|e| e.parent.is_some() || e.nested_cpu_secs != 0.0);
        let version = if nested {
            NESTED_SCHEMA_VERSION
        } else if managed {
            MANAGED_SCHEMA_VERSION
        } else {
            SCHEMA_VERSION
        };
        let shape = |v| (version == v).then_some(&self.entries);
        let wire = Wire {
            version,
            next_seq: self.next_seq,
            entries: shape(SCHEMA_VERSION),
            entries_v2: shape(MANAGED_SCHEMA_VERSION),
            entries_v3: shape(NESTED_SCHEMA_VERSION),
            extra: self.extra.clone(),
        };
        let mut tmp = crate::paths::temp_file_in(dir)?;
        serde_json::to_writer_pretty(&mut tmp, &wire)?;
        tmp.write_all(b"\n")?;
        tmp.as_file().sync_all()?;
        tmp.persist(path).map_err(|e| e.error)?;
        Ok(())
    }

    pub fn take_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    /// What admission sees of every entry, with waiting times as of now.
    pub fn queue(&self) -> Vec<QueueEntry> {
        self.queue_at(Utc::now())
    }

    /// [`Ledger::queue`] as of `now`.
    pub fn queue_at(&self, now: DateTime<Utc>) -> Vec<QueueEntry> {
        self.entries
            .iter()
            .map(|e| QueueEntry {
                id: e.id.clone(),
                state: e.state,
                front: e.front,
                weight: e.weight,
                seq: e.seq,
                using: e.using,
                waited_secs: match e.state {
                    State::Waiting => {
                        (now - e.enqueued_at).num_milliseconds().max(0) as f64 / 1000.0
                    }
                    State::Running => 0.0,
                },
                expected_secs: e.expected_secs,
                backfilled: e.backfilled,
                parent: e.parent.clone(),
                ran_secs: match (e.state, e.started_at) {
                    (State::Running, Some(at)) => {
                        (now - at).num_milliseconds().max(0) as f64 / 1000.0
                    }
                    _ => 0.0,
                },
            })
            .collect()
    }

    /// The entries whose `parent` chain leads back to themselves. `exec`
    /// cannot write one (a parent runs before its child exists); admission
    /// counts such entries for nothing, so `status` warns about them.
    pub fn parent_cycles(&self) -> Vec<&str> {
        let in_cycle = |e: &Entry| {
            let mut at = e.parent.as_deref();
            for _ in 0..self.entries.len() {
                match at {
                    Some(p) if p == e.id => return true,
                    Some(p) => at = self.find(p).and_then(|q| q.parent.as_deref()),
                    None => return false,
                }
            }
            false
        };
        self.entries
            .iter()
            .filter(|e| in_cycle(e))
            .map(|e| e.id.as_str())
            .collect()
    }

    pub fn find(&self, id: &str) -> Option<&Entry> {
        self.entries.iter().find(|e| e.id == id)
    }

    pub fn find_mut(&mut self, id: &str) -> Option<&mut Entry> {
        self.entries.iter_mut().find(|e| e.id == id)
    }

    pub fn remove(&mut self, id: &str) -> bool {
        let before = self.entries.len();
        self.entries.retain(|e| e.id != id);
        before != self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, seq: u64) -> Entry {
        Entry {
            id: id.into(),
            pid: 1,
            state: State::Waiting,
            front: false,
            weight: 1,
            predicted_rss: 0,
            expected_secs: None,
            repo: String::new(),
            subdir: String::new(),
            argv: "x".into(),
            key: "k".into(),
            seq,
            enqueued_at: Utc::now(),
            started_at: None,
            using: None,
            backfilled: false,
            label: None,
            worktree: None,
            branch: None,
            parent: None,
            nested_cpu_secs: 0.0,
            extra: Default::default(),
        }
    }

    /// The ledger types of `origin/main` before managed entries existed,
    /// copied as they were.
    mod old {
        #![allow(dead_code)]
        use crate::admit::State;
        use chrono::{DateTime, Utc};
        use serde::Deserialize;

        #[derive(Debug, Deserialize)]
        pub struct Entry {
            pub id: String,
            pub pid: u32,
            pub state: State,
            pub front: bool,
            pub weight: u32,
            pub predicted_rss: u64,
            #[serde(default)]
            pub expected_secs: Option<f64>,
            pub repo: String,
            #[serde(default)]
            pub subdir: String,
            pub argv: String,
            pub key: String,
            pub seq: u64,
            pub enqueued_at: DateTime<Utc>,
            #[serde(default)]
            pub started_at: Option<DateTime<Utc>>,
            #[serde(default)]
            pub using: Option<f64>,
            #[serde(default)]
            pub backfilled: bool,
        }

        #[derive(Debug, Deserialize)]
        pub struct Ledger {
            pub version: u32,
            pub next_seq: u64,
            pub entries: Vec<Entry>,
        }
    }

    fn save_with_sidecars(l: &Ledger, dir: &Path, managed: &[&str]) -> Vec<u8> {
        std::fs::create_dir_all(dir.join("managed")).unwrap();
        for id in managed {
            std::fs::write(dir.join("managed").join(format!("{id}.json")), b"{}").unwrap();
        }
        l.save(&dir.join("ledger.json")).unwrap();
        std::fs::read(dir.join("ledger.json")).unwrap()
    }

    #[test]
    fn an_older_binary_reads_a_ledger_without_managed_entries() {
        let d = tempfile::tempdir().unwrap();
        let mut l = Ledger::default();
        l.entries.push(entry("a", 1));
        let json = save_with_sidecars(&l, d.path(), &[]);
        let old: old::Ledger = serde_json::from_slice(&json).unwrap();
        assert_eq!(old.version, SCHEMA_VERSION);
        assert_eq!(old.entries.len(), 1);
        let json = save_with_sidecars(&Ledger::default(), d.path(), &[]);
        assert!(serde_json::from_slice::<old::Ledger>(&json).is_ok());
    }

    #[test]
    fn a_label_is_additive_and_older_binaries_ignore_it() {
        let d = tempfile::tempdir().unwrap();
        let mut l = Ledger::default();
        let mut e = entry("a", 1);
        e.label = Some("web/api unit tests".into());
        l.entries.push(e);
        l.entries.push(entry("b", 2));
        let json = save_with_sidecars(&l, d.path(), &[]);
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["version"], SCHEMA_VERSION);
        assert_eq!(v["entries"][0]["label"], "web/api unit tests");
        assert!(v["entries"][1].get("label").is_none());
        let old: old::Ledger = serde_json::from_slice(&json).unwrap();
        assert_eq!(old.entries.len(), 2);
        // And a ledger written before labels existed still loads.
        let back = Ledger::load(&d.path().join("ledger.json")).unwrap();
        assert_eq!(back, l);
    }

    #[test]
    fn a_worktree_is_additive_and_older_binaries_ignore_it() {
        let d = tempfile::tempdir().unwrap();
        let mut l = Ledger::default();
        let mut e = entry("a", 1);
        e.worktree = Some("/w/feature".into());
        e.branch = Some("feat/x".into());
        l.entries.push(e);
        l.entries.push(entry("b", 2));
        let json = save_with_sidecars(&l, d.path(), &[]);
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["version"], SCHEMA_VERSION);
        assert_eq!(v["entries"][0]["worktree"], "/w/feature");
        assert_eq!(v["entries"][0]["branch"], "feat/x");
        assert!(v["entries"][1].get("worktree").is_none());
        assert!(v["entries"][1].get("branch").is_none());
        let old: old::Ledger = serde_json::from_slice(&json).unwrap();
        assert_eq!(old.entries.len(), 2);
        // A ledger written before worktrees were recorded still loads.
        let back = Ledger::load(&d.path().join("ledger.json")).unwrap();
        assert_eq!(back, l);
        let mut bare = serde_json::to_value(&l).unwrap();
        for e in bare["entries"].as_array_mut().unwrap() {
            e.as_object_mut().unwrap().remove("worktree");
            e.as_object_mut().unwrap().remove("branch");
        }
        let parsed: Ledger = serde_json::from_value(bare).unwrap();
        assert!(parsed
            .entries
            .iter()
            .all(|e| e.worktree.is_none() && e.branch.is_none()));
    }

    #[test]
    fn the_format_on_disk_is_reported() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("ledger.json");
        assert_eq!(Ledger::file_format(&path), SCHEMA_VERSION);
        let mut l = Ledger::default();
        l.entries.push(entry("m", 1));
        save_with_sidecars(&l, d.path(), &["m"]);
        assert_eq!(Ledger::file_format(&path), MANAGED_SCHEMA_VERSION);
        save_with_sidecars(&l, tempfile::tempdir().unwrap().path(), &[]);
        std::fs::write(&path, b"not json").unwrap();
        assert_eq!(Ledger::file_format(&path), SCHEMA_VERSION);
    }

    #[test]
    fn a_managed_directory_that_cannot_be_checked_counts_as_managed() {
        let d = tempfile::tempdir().unwrap();
        let mut l = Ledger::default();
        l.entries.push(entry("a", 1));
        // `managed` is a file, so looking inside it is an error (not "no
        // such file"): the ledger must be written as v2.
        std::fs::write(d.path().join("managed"), b"").unwrap();
        l.save(&d.path().join("ledger.json")).unwrap();
        let json = std::fs::read(d.path().join("ledger.json")).unwrap();
        assert!(serde_json::from_slice::<old::Ledger>(&json).is_err());
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["version"], MANAGED_SCHEMA_VERSION);
        // No managed directory at all is a plain ledger.
        let e = tempfile::tempdir().unwrap();
        let json = save_with_sidecars(&l, e.path(), &[]);
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["version"], SCHEMA_VERSION);
    }

    #[test]
    fn an_older_binary_refuses_a_ledger_with_a_managed_entry() {
        let d = tempfile::tempdir().unwrap();
        let mut l = Ledger::default();
        l.entries.push(entry("a", 1));
        l.entries.push(entry("m", 2));
        let json = save_with_sidecars(&l, d.path(), &["m"]);
        let err = serde_json::from_slice::<old::Ledger>(&json).unwrap_err();
        assert!(err.to_string().contains("entries"), "{err}");
        // The current code reads it back whole, and the plain-JSON view
        // (status --json) is unchanged.
        let back = Ledger::load(&d.path().join("ledger.json")).unwrap();
        assert_eq!(back.entries.len(), 2);
        let shown = serde_json::to_value(&back).unwrap();
        assert_eq!(shown["entries"].as_array().unwrap().len(), 2);
        // It drops to the old shape when the managed entry is gone.
        let mut back = back;
        back.remove("m");
        std::fs::remove_file(d.path().join("managed/m.json")).unwrap();
        let json = save_with_sidecars(&back, d.path(), &[]);
        assert!(serde_json::from_slice::<old::Ledger>(&json).is_ok());
    }

    #[test]
    fn a_ledger_with_both_or_neither_entries_key_is_rejected() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("ledger.json");
        for body in [
            r#"{"version":1,"next_seq":1}"#,
            r#"{"version":1,"next_seq":1,"entries":[],"entries_v2":[]}"#,
        ] {
            std::fs::write(&p, body).unwrap();
            assert!(Ledger::load(&p).is_err(), "{body}");
        }
    }

    /// How the last release before nested admission reads the file: one
    /// of `entries` and `entries_v2`, anything else refused.
    fn read_by_0_8(path: &Path) -> std::result::Result<usize, String> {
        #[derive(Deserialize)]
        struct OldWire {
            #[serde(default)]
            entries: Option<Vec<serde_json::Value>>,
            #[serde(default)]
            entries_v2: Option<Vec<serde_json::Value>>,
        }
        let w: OldWire =
            serde_json::from_slice(&std::fs::read(path).unwrap()).map_err(|e| e.to_string())?;
        match (w.entries, w.entries_v2) {
            (Some(e), None) | (None, Some(e)) => Ok(e.len()),
            _ => Err("a ledger has exactly one of `entries` and `entries_v2`".into()),
        }
    }

    #[test]
    fn a_ledger_with_a_nested_entry_is_one_older_binaries_refuse() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ledger.json");
        let mut l = Ledger::default();
        l.entries.push(entry("p", 1));
        l.save(&path).unwrap();
        assert_eq!(Ledger::file_format(&path), SCHEMA_VERSION);
        assert_eq!(read_by_0_8(&path), Ok(1));

        // A child: an older binary would write the file back without
        // `parent`, so it must not be able to read it.
        let mut child = entry("c", 2);
        child.parent = Some("p".into());
        l.entries.push(child);
        l.save(&path).unwrap();
        assert_eq!(Ledger::file_format(&path), NESTED_SCHEMA_VERSION);
        assert!(read_by_0_8(&path).is_err());
        assert_eq!(Ledger::load(&path).unwrap().entries, l.entries);

        // The child is gone but the parent still carries what it used.
        l.entries.pop();
        l.entries[0].nested_cpu_secs = 2.5;
        l.save(&path).unwrap();
        assert_eq!(Ledger::file_format(&path), NESTED_SCHEMA_VERSION);
        assert!(read_by_0_8(&path).is_err());
        assert_eq!(Ledger::load(&path).unwrap().entries, l.entries);

        // Nothing nested any more: the old shape again.
        l.entries[0].nested_cpu_secs = 0.0;
        l.save(&path).unwrap();
        assert_eq!(Ledger::file_format(&path), SCHEMA_VERSION);
        assert_eq!(read_by_0_8(&path), Ok(1));
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("nested_cpu_secs") && !text.contains("parent"));
    }

    #[test]
    fn a_file_with_two_shapes_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ledger.json");
        std::fs::write(
            &path,
            r#"{"version":3,"next_seq":1,"entries":[],"entries_v3":[]}"#,
        )
        .unwrap();
        assert!(Ledger::load(&path).is_err());
        std::fs::write(&path, r#"{"version":3,"next_seq":1}"#).unwrap();
        assert!(Ledger::load(&path).is_err());
    }

    #[test]
    fn a_running_entry_reports_how_long_it_has_run() {
        let now = Utc::now();
        let mut running = entry("r", 1);
        running.state = State::Running;
        running.started_at = Some(now - chrono::Duration::seconds(90));
        let mut unstamped = entry("u", 2);
        unstamped.state = State::Running;
        let mut waiting = entry("w", 3);
        waiting.enqueued_at = now - chrono::Duration::seconds(30);
        // Never read for a waiting entry.
        waiting.started_at = Some(now - chrono::Duration::seconds(30));
        let l = Ledger {
            entries: vec![running, unstamped, waiting],
            ..Ledger::default()
        };
        let q = l.queue_at(now);
        assert_eq!(
            q.iter().map(|e| e.ran_secs).collect::<Vec<_>>(),
            [90.0, 0.0, 0.0]
        );
        assert_eq!(q[2].waited_secs, 30.0);
    }

    #[test]
    fn entries_that_name_each_other_as_parent_are_found() {
        let with_parent = |id: &str, seq, parent: &str| {
            let mut e = entry(id, seq);
            e.parent = Some(parent.into());
            e
        };
        let mut l = Ledger {
            entries: vec![
                entry("p", 1),
                with_parent("c", 2, "p"),
                with_parent("g", 3, "c"),
                with_parent("orphan", 4, "gone"),
            ],
            ..Ledger::default()
        };
        assert!(l.parent_cycles().is_empty());
        l.entries.extend([
            with_parent("a", 5, "b"),
            with_parent("b", 6, "a"),
            with_parent("self", 7, "self"),
            // Hangs off a cycle without being part of it.
            with_parent("below", 8, "a"),
        ]);
        assert_eq!(l.parent_cycles(), ["a", "b", "self"]);
    }

    #[test]
    fn fields_this_binary_does_not_know_survive_a_rewrite() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("ledger.json");
        let mut e = serde_json::to_value(entry("a", 1)).unwrap();
        e["future"] = serde_json::json!({"nested": {"n": [1, 2, {"k": null}]}});
        e["worktree"] = "/w".into();
        let file = serde_json::json!({
            "version": 1, "next_seq": 2, "entries": [e],
            "top_future": {"a": {"b": 1}},
        });
        std::fs::write(&path, file.to_string()).unwrap();
        let mut l = Ledger::load(&path).unwrap();
        // Known fields are not also kept as unknown ones.
        for k in ["worktree", "id", "entries", "version", "next_seq"] {
            assert!(!l.extra.contains_key(k) && !l.entries[0].extra.contains_key(k));
        }
        assert_eq!(l.entries[0].extra.len(), 1);
        l.entries[0].state = State::Running;
        l.entries[0].branch = Some("b".into());
        l.save(&path).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.matches("\"worktree\"").count(), 1);
        let back: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(back["top_future"], file["top_future"]);
        assert_eq!(back["entries"][0]["future"], e["future"]);
        assert_eq!(back["entries"][0]["worktree"], "/w");
        assert_eq!(back["entries"][0]["state"], "running");
        assert_eq!(back["entries"][0]["branch"], "b");
        assert_eq!(back["version"], 1);
    }

    #[test]
    fn unknown_fields_survive_a_shape_change() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("ledger.json");
        let mut l = Ledger::default();
        l.extra.insert("top_future".into(), 7.into());
        let mut e = entry("a", 1);
        e.parent = Some("p".into());
        e.extra.insert("future".into(), "x".into());
        l.entries.push(e);
        l.save(&path).unwrap();
        let back: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(back["version"], 3);
        assert_eq!(back["top_future"], 7);
        assert_eq!(back["entries_v3"][0]["future"], "x");
        let loaded = Ledger::load(&path).unwrap();
        assert_eq!((loaded.entries, loaded.extra), (l.entries, l.extra));
    }

    #[test]
    fn round_trip() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("sub/ledger.json");
        let mut l = Ledger::default();
        let s = l.take_seq();
        l.entries.push(entry("a", s));
        l.save(&p).unwrap();
        let back = Ledger::load(&p).unwrap();
        assert_eq!(back, l);
        assert_eq!(back.next_seq, 2);
        assert!(back.find("a").is_some());
    }

    #[test]
    fn missing_and_empty_are_default() {
        let d = tempfile::tempdir().unwrap();
        assert_eq!(
            Ledger::load(&d.path().join("x")).unwrap(),
            Ledger::default()
        );
        let p = d.path().join("empty");
        std::fs::write(&p, b"").unwrap();
        assert_eq!(Ledger::load(&p).unwrap(), Ledger::default());
    }

    #[test]
    fn remove_reports_presence() {
        let mut l = Ledger::default();
        l.entries.push(entry("a", 1));
        assert!(l.remove("a"));
        assert!(!l.remove("a"));
    }
}
