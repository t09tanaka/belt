//! Append-only measurement history (`history.jsonl`).

use crate::error::Result;
use crate::weight::Measurement;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Record {
    pub key: String,
    pub repo: String,
    #[serde(default)]
    pub subdir: String,
    pub argv: String,
    /// What the run was keyed by, when that is not `argv` (a command found
    /// inside `sh -c` or a temporary script).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key_argv: String,
    pub started_at: DateTime<Utc>,
    pub wall_secs: f64,
    pub cpu_secs: f64,
    pub max_rss_bytes: u64,
    pub exit_code: i32,
    pub weight_used: u32,
    /// CPU was above the threshold when the run started.
    #[serde(default)]
    pub contended: bool,
    /// Set by `managed::record_measurement`: the id that applied this
    /// record. History written before it existed has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurement_id: Option<String>,
    /// Fields this binary does not know, kept so that rewriting the file
    /// does not drop what a newer binary wrote.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl Record {
    pub fn measurement(&self) -> Measurement {
        Measurement {
            wall_secs: self.wall_secs,
            cpu_secs: self.cpu_secs,
            max_rss_bytes: self.max_rss_bytes,
            contended: self.contended,
        }
    }
}

/// How long an append or a trim waits for the other on the history file
/// itself. Both hold it for a few system calls only.
const FILE_LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(1);
const FILE_LOCK_POLL: std::time::Duration = std::time::Duration::from_millis(2);

/// Append `rec` as one line. The append holds a shared lock on the file it
/// writes to, and writes only once that file is still the one `path`
/// names: a trim replaces the file under an exclusive lock on the old one
/// taken before its last look ([`trim_collect`]), so an append either lands
/// before that last look and is carried over, or finds the file replaced
/// and goes to the new one. This holds with or without the history lock.
/// Only an append that waits [`FILE_LOCK_WAIT`] on a trim stopped inside
/// that window writes without the lock, and can then be lost.
pub fn append(path: &Path, rec: &Record) -> Result<()> {
    append_with(path, rec, &mut || {})
}

/// [`append`], calling `after_open` once the file is open and before it is
/// locked (a seam for tests).
fn append_with(path: &Path, rec: &Record, after_open: &mut dyn FnMut()) -> Result<()> {
    use fs2::FileExt;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut line = serde_json::to_string(rec)?;
    line.push('\n');
    let start = std::time::Instant::now();
    loop {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        after_open();
        let locked = loop {
            match FileExt::try_lock_shared(&f) {
                Ok(()) => break true,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if start.elapsed() >= FILE_LOCK_WAIT {
                        break false;
                    }
                    std::thread::sleep(FILE_LOCK_POLL);
                }
                Err(e) => return Err(e.into()),
            }
        };
        if locked && !crate::lock::names(&f, path) {
            // Replaced (or removed) by a trim since it was opened.
            let _ = FileExt::unlock(&f);
            continue;
        }
        let out = f.write_all(line.as_bytes());
        if locked {
            // Explicitly: a child forked meanwhile holds a copy of the fd.
            let _ = FileExt::unlock(&f);
        }
        out?;
        return Ok(());
    }
}

/// All records, oldest first. Malformed lines are skipped.
pub fn read_all(path: &Path) -> Result<Vec<Record>> {
    let f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(r) = serde_json::from_str::<Record>(&line) {
            out.push(r);
        }
    }
    Ok(out)
}

pub fn for_key(path: &Path, key: &str) -> Result<Vec<Record>> {
    Ok(read_all(path)?
        .into_iter()
        .filter(|r| r.key == key)
        .collect())
}

/// Keep only the newest `keep` records per key. Returns records dropped.
pub fn trim(path: &Path, keep: usize) -> Result<usize> {
    trim_aged(path, keep, None)
}

/// [`trim`], and with `cutoff` also drop every record of a key whose newest
/// record started before it (a command nobody has run since). A key that
/// was seen after `cutoff` keeps its newest `keep` records whatever their
/// age. Returns records dropped.
pub fn trim_aged(path: &Path, keep: usize, cutoff: Option<DateTime<Utc>>) -> Result<usize> {
    Ok(trim_collect(path, keep, cutoff)?.0)
}

/// Which of `all` (oldest first) a trim keeps, as indexes, and the
/// `measurement_id`s that no kept record carries any more. An id that a
/// kept record still carries is not returned, even when another record with
/// it is dropped: its marker is what stops the measurement from being
/// applied again.
pub fn trim_plan(
    all: &[Record],
    keep: usize,
    cutoff: Option<DateTime<Utc>>,
) -> (Vec<usize>, Vec<String>) {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    let mut newest: HashMap<&str, DateTime<Utc>> = HashMap::new();
    for r in all {
        *counts.entry(r.key.as_str()).or_default() += 1;
        let t = newest.entry(r.key.as_str()).or_insert(r.started_at);
        if r.started_at > *t {
            *t = r.started_at;
        }
    }
    let mut seen: HashMap<&str, usize> = HashMap::new();
    let mut kept = Vec::new();
    let mut dropped = Vec::new();
    for (i, r) in all.iter().enumerate() {
        let n = seen.entry(r.key.as_str()).or_default();
        *n += 1;
        let stale = cutoff.is_some_and(|c| newest[r.key.as_str()] < c);
        if !stale && counts[r.key.as_str()] - *n < keep {
            kept.push(i);
        } else {
            dropped.push(i);
        }
    }
    let still: std::collections::HashSet<&str> = kept
        .iter()
        .filter_map(|&i| all[i].measurement_id.as_deref())
        .collect();
    let mut gone: Vec<String> = Vec::new();
    for i in dropped {
        if let Some(id) = all[i].measurement_id.as_deref() {
            if !still.contains(id) && !gone.iter().any(|g| g == id) {
                gone.push(id.to_string());
            }
        }
    }
    (kept, gone)
}

fn parse(bytes: &[u8]) -> Vec<Record> {
    bytes
        .split(|&b| b == b'\n')
        .filter_map(|line| serde_json::from_slice::<Record>(line).ok())
        .collect()
}

/// How many of `bytes` are whole lines: up to and including the last
/// newline. The rest is a line that is not finished.
pub fn whole_lines(bytes: &[u8]) -> usize {
    bytes.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1)
}

/// What was appended to `path` after its first `from` bytes.
fn appended_since(path: &Path, from: u64) -> Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path)?;
    f.seek(SeekFrom::Start(from))?;
    let mut out = Vec::new();
    f.read_to_end(&mut out)?;
    Ok(out)
}

/// [`trim_aged`], also returning the `measurement_id` of every record it
/// dropped for good ([`trim_plan`]), so the caller can remove their markers
/// at once. A history left with no record is removed, not kept as an empty
/// file.
///
/// The file is replaced by a new one. What was appended while the new file
/// was being written is carried over: the last look is taken under an
/// exclusive lock on the old file, held until it is replaced, so an
/// [`append`] of this version cannot land after it (it waits, sees the file
/// replaced and writes to the new one), with or without the history lock.
/// When an append holds that lock for [`FILE_LOCK_WAIT`] the trim does
/// nothing. A released version appends without the file lock: its append
/// that lands between the last look and the rename is lost, and its line
/// still half written at the last look is cut off (its writer holds the old
/// file, so nothing could finish it).
pub fn trim_collect(
    path: &Path,
    keep: usize,
    cutoff: Option<DateTime<Utc>>,
) -> Result<(usize, Vec<String>)> {
    trim_collect_with(path, keep, cutoff, &mut || {}, &mut || {})
}

/// [`trim_collect`], calling `before_last_look` after the kept lines are
/// written and before what was appended meanwhile is carried over (a seam
/// for tests), and `before_replace` after that last look, with the old file
/// locked, before it is replaced.
fn trim_collect_with(
    path: &Path,
    keep: usize,
    cutoff: Option<DateTime<Utc>>,
    before_last_look: &mut dyn FnMut(),
    before_replace: &mut dyn FnMut(),
) -> Result<(usize, Vec<String>)> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, Vec::new())),
        Err(e) => return Err(e.into()),
    };
    // Only whole lines are records. What follows the last newline is a
    // line somebody is still writing (a released version appends without
    // the lock): it is carried over with whatever comes after, as far as it
    // is whole at the last look.
    let whole = whole_lines(&bytes);
    let all = parse(&bytes[..whole]);
    let (kept, gone_ids) = trim_plan(&all, keep, cutoff);
    let dropped = all.len() - kept.len();
    if dropped == 0 {
        return Ok((0, gone_ids));
    }
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = crate::paths::temp_file_in(dir)?;
    // How much of `tmp` is written, and how much of it ends at a newline.
    let mut written = 0u64;
    for &i in &kept {
        let mut line = serde_json::to_string(&all[i])?;
        line.push('\n');
        tmp.write_all(line.as_bytes())?;
        written += line.len() as u64;
    }
    let mut cut = written;
    before_last_look();
    // From here until the file is replaced, no append of this version can
    // write to it ([`append`]): what the last look sees is everything.
    let Some(old) = lock_exclusive_named(path)? else {
        // An append holds it longer than it ever should: trim another time.
        return Ok((0, Vec::new()));
    };
    let mut at = whole as u64;
    loop {
        let more = appended_since(path, at)?;
        if more.is_empty() {
            break;
        }
        tmp.write_all(&more)?;
        if let Some(i) = more.iter().rposition(|&b| b == b'\n') {
            cut = written + i as u64 + 1;
        }
        written += more.len() as u64;
        at += more.len() as u64;
    }
    // Nothing can complete a line that is still half written here: its
    // writer holds the old file. Left in, it would merge with the next
    // append into one malformed line and lose that record too.
    if cut < written {
        tmp.as_file_mut().set_len(cut)?;
    }
    before_replace();
    if cut == 0 {
        std::fs::remove_file(path)?;
    } else {
        tmp.persist(path).map_err(|e| e.error)?;
    }
    let _ = fs2::FileExt::unlock(&old);
    Ok((dropped, gone_ids))
}

/// An exclusive lock on the file `path` names, waiting at most
/// [`FILE_LOCK_WAIT`]; `None` when it stays busy or `path` is gone.
fn lock_exclusive_named(path: &Path) -> Result<Option<std::fs::File>> {
    use fs2::FileExt;
    let f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let start = std::time::Instant::now();
    loop {
        match FileExt::try_lock_exclusive(&f) {
            Ok(()) => break,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if start.elapsed() >= FILE_LOCK_WAIT {
                    return Ok(None);
                }
                std::thread::sleep(FILE_LOCK_POLL);
            }
            Err(e) => return Err(e.into()),
        }
    }
    // Under the history lock nobody else replaces it; a released version
    // does not take this lock, and then the file may already be another.
    if !crate::lock::names(&f, path) {
        let _ = FileExt::unlock(&f);
        return Ok(None);
    }
    Ok(Some(f))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(key: &str, wall: f64) -> Record {
        Record {
            key: key.into(),
            repo: String::new(),
            subdir: String::new(),
            argv: "x".into(),
            key_argv: String::new(),
            started_at: Utc::now(),
            wall_secs: wall,
            cpu_secs: 1.0,
            max_rss_bytes: 1,
            exit_code: 0,
            weight_used: 1,
            contended: false,
            measurement_id: None,
            extra: Default::default(),
        }
    }

    #[test]
    fn append_read_filter() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("h/history.jsonl");
        append(&p, &rec("a", 1.0)).unwrap();
        append(&p, &rec("b", 2.0)).unwrap();
        append(&p, &rec("a", 3.0)).unwrap();
        let a = for_key(&p, "a").unwrap();
        assert_eq!(a.len(), 2);
        assert_eq!(a[1].wall_secs, 3.0);
        assert!(read_all(&d.path().join("missing")).unwrap().is_empty());
    }

    #[test]
    fn history_without_a_measurement_id_still_reads_and_none_is_not_written() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("history.jsonl");
        let old = r#"{"key":"k","repo":"r","argv":"x","started_at":"2026-01-01T00:00:00Z","wall_secs":1.0,"cpu_secs":1.0,"max_rss_bytes":1,"exit_code":0,"weight_used":1}"#;
        std::fs::write(&p, format!("{old}\n")).unwrap();
        let all = read_all(&p).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].measurement_id, None);
        append(&p, &rec("a", 1.0)).unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        assert!(!text.contains("measurement_id"), "{text}");
        let mut with_id = rec("b", 1.0);
        with_id.measurement_id = Some("m-1".into());
        append(&p, &with_id).unwrap();
        let back = read_all(&p).unwrap();
        assert_eq!(back[2].measurement_id.as_deref(), Some("m-1"));
    }

    #[test]
    fn malformed_lines_are_skipped() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("history.jsonl");
        std::fs::write(&p, "garbage\n\n").unwrap();
        append(&p, &rec("a", 1.0)).unwrap();
        assert_eq!(read_all(&p).unwrap().len(), 1);
    }

    #[test]
    fn trim_keeps_fields_it_does_not_know() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("h.jsonl");
        let mut v = serde_json::to_value(rec("k", 1.0)).unwrap();
        v["future"] = serde_json::json!({"a": [1]});
        for _ in 0..3 {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&p)
                .unwrap()
                .write_all(format!("{v}\n").as_bytes())
                .unwrap();
        }
        assert_eq!(trim(&p, 2).unwrap(), 1);
        let text = std::fs::read_to_string(&p).unwrap();
        assert_eq!(text.lines().count(), 2);
        for line in text.lines() {
            let got: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(got, v);
        }
    }

    #[test]
    fn a_dropped_record_keeps_its_marker_while_a_kept_record_has_the_same_id() {
        let with = |key: &str, id: Option<&str>| {
            let mut r = rec(key, 1.0);
            r.measurement_id = id.map(str::to_string);
            r
        };
        // "dup" was appended twice (a resend that slipped through): the
        // older copy is dropped, the newer one is kept, so its marker stays.
        // "old" is only in a dropped record. "kept" is only in a kept one.
        let all = [
            with("a", Some("dup")),
            with("a", Some("old")),
            with("a", Some("old")),
            with("a", None),
            with("a", Some("dup")),
            with("a", Some("kept")),
        ];
        let (kept, gone) = trim_plan(&all, 2, None);
        assert_eq!(kept, [4, 5]);
        assert_eq!(gone, ["old"]);
        // Nothing dropped, nothing gone.
        assert_eq!(trim_plan(&all, 6, None), ((0..6).collect(), vec![]));
    }

    #[test]
    fn trim_carries_over_what_was_appended_without_the_lock_and_keeps_such_a_file() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("history.jsonl");
        append(&p, &rec("a", 1.0)).unwrap();
        append(&p, &rec("a", 2.0)).unwrap();
        let len = std::fs::metadata(&p).unwrap().len();
        append(&p, &rec("a", 3.0)).unwrap();
        let more = appended_since(&p, len).unwrap();
        assert_eq!(parse(&more).len(), 1);
        assert_eq!(parse(&more)[0].wall_secs, 3.0);
        assert!(appended_since(&p, std::fs::metadata(&p).unwrap().len())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn whole_lines_end_at_the_last_newline() {
        assert_eq!(whole_lines(b""), 0);
        assert_eq!(whole_lines(b"abc"), 0);
        assert_eq!(whole_lines(b"abc\n"), 4);
        assert_eq!(whole_lines(b"abc\nde"), 4);
        assert_eq!(whole_lines(b"\n"), 1);
    }

    /// The tail of a line that was half written when the trim looked, and
    /// arrives before the trim's last look, is carried over whole.
    #[test]
    fn a_line_caught_half_written_is_carried_over_whole() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("h.jsonl");
        for w in [1.0, 2.0, 3.0] {
            append(&p, &rec("k", w)).unwrap();
        }
        let mut line = serde_json::to_string(&rec("k", 4.0)).unwrap();
        line.push('\n');
        let (head, tail) = line.split_at(line.len() / 2);
        let add = |text: &str| {
            let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
            f.write_all(text.as_bytes()).unwrap();
        };
        add(head);
        let (dropped, _) = trim_collect_with(&p, 1, None, &mut || add(tail), &mut || {}).unwrap();
        assert_eq!(dropped, 2);
        let walls: Vec<f64> = read_all(&p).unwrap().iter().map(|r| r.wall_secs).collect();
        assert_eq!(walls, vec![3.0, 4.0]);
        let text = std::fs::read_to_string(&p).unwrap();
        assert_eq!(text.lines().count(), 2, "{text}");

        // A file of whole lines that are all dropped is removed.
        let r = dir.path().join("r.jsonl");
        append(&r, &rec("k", 1.0)).unwrap();
        assert_eq!(trim(&r, 0).unwrap(), 1);
        assert!(!r.exists());
    }

    /// An append that opened the file before a trim's last look and writes
    /// after it, while the trim has not replaced the file yet, is not lost:
    /// it waits for the replacement and writes to the new file.
    #[test]
    fn an_append_between_the_last_look_and_the_replacement_goes_to_the_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("h.jsonl");
        for w in [1.0, 2.0, 3.0] {
            append(&p, &rec("k", w)).unwrap();
        }
        let mut writer = None;
        let (dropped, _) = trim_collect_with(&p, 1, None, &mut || {}, &mut || {
            // The writer opens the old file now, past the last look, and
            // writes whenever it can.
            let (opened_tx, opened_rx) = std::sync::mpsc::channel();
            let path = p.clone();
            writer = Some(std::thread::spawn(move || {
                append_with(&path, &rec("late", 9.0), &mut || {
                    let _ = opened_tx.send(());
                })
                .unwrap();
            }));
            opened_rx.recv().unwrap();
        })
        .unwrap();
        writer.unwrap().join().unwrap();
        assert_eq!(dropped, 2);
        let walls: Vec<f64> = read_all(&p).unwrap().iter().map(|r| r.wall_secs).collect();
        assert_eq!(walls, vec![3.0, 9.0]);
    }

    /// A half line nobody finishes (a writer that died, or one whose tail
    /// lands in the old file after the trim's last look) must not be
    /// persisted: the next append would merge with it and be lost.
    #[test]
    fn a_half_line_nobody_finishes_is_not_kept() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("h.jsonl");
        for w in [1.0, 2.0, 3.0] {
            append(&p, &rec("k", w)).unwrap();
        }
        let mut line = serde_json::to_string(&rec("k", 4.0)).unwrap();
        line.push('\n');
        let head = &line[..line.len() / 2];
        let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
        f.write_all(head.as_bytes()).unwrap();
        drop(f);
        assert_eq!(trim(&p, 1).unwrap(), 2);
        let text = std::fs::read_to_string(&p).unwrap();
        assert!(text.ends_with('\n'), "{text:?}");
        append(&p, &rec("k", 5.0)).unwrap();
        let walls: Vec<f64> = read_all(&p).unwrap().iter().map(|r| r.wall_secs).collect();
        assert_eq!(walls, vec![3.0, 5.0]);

        // With nothing kept, the half line alone does not keep the file.
        let q = dir.path().join("q.jsonl");
        append(&q, &rec("k", 1.0)).unwrap();
        let mut f = std::fs::OpenOptions::new().append(true).open(&q).unwrap();
        f.write_all(head.as_bytes()).unwrap();
        drop(f);
        assert_eq!(trim(&q, 0).unwrap(), 1);
        assert!(!q.exists());
    }

    #[test]
    fn trim_keeps_newest_per_key() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("history.jsonl");
        for i in 0..5 {
            append(&p, &rec("a", i as f64)).unwrap();
        }
        append(&p, &rec("b", 9.0)).unwrap();
        assert_eq!(trim(&p, 2).unwrap(), 3);
        let a = for_key(&p, "a").unwrap();
        assert_eq!(
            a.iter().map(|r| r.wall_secs).collect::<Vec<_>>(),
            [3.0, 4.0]
        );
        assert_eq!(for_key(&p, "b").unwrap().len(), 1);
        assert_eq!(trim(&p, 2).unwrap(), 0);
    }

    #[test]
    fn trim_aged_drops_keys_not_seen_since_the_cutoff_and_nothing_else() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("history.jsonl");
        let at = |days_ago: i64| Utc::now() - chrono::Duration::days(days_ago);
        let put = |key: &str, days_ago: i64| {
            let mut r = rec(key, 1.0);
            r.started_at = at(days_ago);
            append(&p, &r).unwrap();
        };
        // "old": every record before the cutoff. "mixed": one old, one new
        // (the key is still in use, so both stay). "new": recent.
        put("old", 200);
        put("old", 100);
        put("mixed", 200);
        put("mixed", 1);
        put("new", 2);
        let cutoff = Some(at(90));
        assert_eq!(trim_aged(&p, 200, cutoff).unwrap(), 2);
        assert!(for_key(&p, "old").unwrap().is_empty());
        assert_eq!(for_key(&p, "mixed").unwrap().len(), 2);
        assert_eq!(for_key(&p, "new").unwrap().len(), 1);
        assert_eq!(trim_aged(&p, 200, cutoff).unwrap(), 0);
    }

    #[test]
    fn trim_aged_keeps_a_key_whose_newest_record_is_exactly_the_cutoff() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("history.jsonl");
        let cutoff = Utc::now();
        let mut r = rec("edge", 1.0);
        r.started_at = cutoff;
        append(&p, &r).unwrap();
        assert_eq!(trim_aged(&p, 200, Some(cutoff)).unwrap(), 0);
        r.started_at = cutoff - chrono::Duration::seconds(1);
        let q = d.path().join("h2.jsonl");
        append(&q, &r).unwrap();
        assert_eq!(trim_aged(&q, 200, Some(cutoff)).unwrap(), 1);
    }
}
