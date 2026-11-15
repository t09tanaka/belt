//! Measured CPU use of a running command's process tree (spec "Measured
//! usage").
//!
//! The running `task-traffic-controller` samples its own child's process tree from a
//! background thread and writes the cores it uses to its ledger entry, so
//! admission (and `status`) can charge a command that uses more than it
//! declared. Nothing here slows down or stops the command.

use crate::ledger::Ledger;
use crate::{lock, Context};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::Duration;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

/// How often the tree is sampled; each sample is the average over this
/// interval.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// What is reported is the peak of this many recent samples, so a
/// momentary dip (a worker between test files) does not free the budget.
pub const PEAK_WINDOW: usize = 3;

/// The peak of the last [`PEAK_WINDOW`] samples.
#[derive(Debug, Default)]
pub struct RecentPeak {
    samples: VecDeque<f64>,
}

impl RecentPeak {
    /// Add a sample and return the current peak.
    pub fn push(&mut self, cores: f64) -> f64 {
        if self.samples.len() == PEAK_WINDOW {
            self.samples.pop_front();
        }
        self.samples.push_back(cores);
        self.samples.iter().copied().fold(0.0, f64::max)
    }
}

/// One process as far as tree accounting cares.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Proc {
    pub pid: u32,
    pub parent: Option<u32>,
    /// Percent of one core (may exceed 100 for a multi-threaded process).
    pub cpu_percent: f32,
    /// Resident memory, in bytes.
    pub rss: u64,
}

/// `root` and all of its descendants, leaving out the processes in `skip`
/// and everything below them (`root` itself is never left out).
fn tree<'a>(procs: &'a [Proc], root: u32, skip: &HashSet<u32>) -> Vec<&'a Proc> {
    let mut children: HashMap<u32, Vec<&Proc>> = HashMap::new();
    let mut by_pid: HashMap<u32, &Proc> = HashMap::new();
    for p in procs {
        by_pid.insert(p.pid, p);
        if let Some(parent) = p.parent {
            if parent != p.pid {
                children.entry(parent).or_default().push(p);
            }
        }
    }
    let mut seen = HashSet::new();
    let mut stack = vec![root];
    let mut found = Vec::new();
    while let Some(pid) = stack.pop() {
        if !seen.insert(pid) {
            continue;
        }
        if let Some(p) = by_pid.get(&pid) {
            found.push(*p);
        }
        if let Some(kids) = children.get(&pid) {
            stack.extend(kids.iter().map(|k| k.pid).filter(|k| !skip.contains(k)));
        }
    }
    found
}

/// Cores used by `root` and all of its descendants, leaving out the
/// processes in `skip` and everything below them.
pub fn tree_cores(procs: &[Proc], root: u32, skip: &HashSet<u32>) -> f64 {
    tree(procs, root, skip)
        .iter()
        .map(|p| f64::from(p.cpu_percent.max(0.0)) / 100.0)
        .sum()
}

/// The resident size of the largest process among `root` and its
/// descendants (what `ru_maxrss` reports for a tree), leaving out the
/// processes in `skip` and everything below them.
pub fn tree_peak_rss(procs: &[Proc], root: u32, skip: &HashSet<u32>) -> u64 {
    tree(procs, root, skip)
        .iter()
        .map(|p| p.rss)
        .max()
        .unwrap_or(0)
}

/// Samples a process tree until dropped or [`Monitor::finish`]ed.
pub struct Monitor {
    stop: Option<Sender<()>>,
    handle: Option<JoinHandle<Option<u64>>>,
}

impl Monitor {
    /// Sample the tree of `root` for the entry `id`, registered with
    /// `parent` ([`crate::exec::Opts::parent`]).
    pub fn start(ctx: Context, id: String, root: u32, parent: Option<String>) -> Monitor {
        let (tx, rx) = mpsc::channel::<()>();
        // The sampler must never be the thread a SIGTERM / SIGHUP lands
        // on: the main thread relies on it interrupting `wait4`. Block
        // everything while spawning so the new thread inherits the mask.
        let old = block_all_signals();
        let handle = std::thread::Builder::new()
            .name("task-traffic-controller-usage".into())
            .spawn(move || sample_loop(&ctx, &id, root, parent.as_deref(), &rx))
            .ok();
        restore_signals(&old);
        Monitor {
            stop: Some(tx),
            handle,
        }
    }

    /// Stop sampling. When entries were nested in this one: the largest
    /// resident size seen among the command's own processes, theirs left
    /// out (the command's `rusage` covers them too). `None` otherwise.
    pub fn finish(mut self) -> Option<u64> {
        drop(self.stop.take());
        self.handle.take().and_then(|h| h.join().ok()).flatten()
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

pub(crate) fn block_all_signals() -> libc::sigset_t {
    unsafe {
        let mut all: libc::sigset_t = std::mem::zeroed();
        let mut old: libc::sigset_t = std::mem::zeroed();
        libc::sigfillset(&mut all);
        libc::pthread_sigmask(libc::SIG_BLOCK, &all, &mut old);
        old
    }
}

pub(crate) fn restore_signals(old: &libc::sigset_t) {
    unsafe {
        libc::pthread_sigmask(libc::SIG_SETMASK, old, std::ptr::null_mut());
    }
}

fn sample_loop(
    ctx: &Context,
    id: &str,
    root: u32,
    parent: Option<&str>,
    stop: &mpsc::Receiver<()>,
) -> Option<u64> {
    // Do not keep /proc files open between samples (Linux).
    sysinfo::set_open_files_limit(0);
    let kind = ProcessRefreshKind::nothing().with_cpu().with_memory();
    let mut sys = System::new();
    let mut peak = RecentPeak::default();
    let mut recorded = None;
    let (mut own_rss, mut had_nested) = (0, false);
    sys.refresh_processes_specifics(ProcessesToUpdate::All, true, kind);
    loop {
        match stop.recv_timeout(SAMPLE_INTERVAL) {
            Err(RecvTimeoutError::Timeout) => {}
            _ => return had_nested.then_some(own_rss),
        }
        sys.refresh_processes_specifics(ProcessesToUpdate::All, true, kind);
        let procs: Vec<Proc> = sys
            .processes()
            .iter()
            .map(|(pid, p)| Proc {
                pid: pid.as_u32(),
                parent: p.parent().map(|q| q.as_u32()),
                cpu_percent: p.cpu_usage(),
                rss: p.memory(),
            })
            .collect();
        // A plain read: saves are atomic renames, and a stale copy is at
        // worst one sample behind.
        let others = Ledger::load(&ctx.paths.ledger_file())
            .map(|ledger| others(&ledger, id, parent))
            .unwrap_or_default();
        own_rss = own_rss.max(tree_peak_rss(&procs, root, &others.skip));
        had_nested |= others.nested;
        let cores = (peak.push(tree_cores(&procs, root, &others.skip)) * 10.0).round() / 10.0;
        // An unchanged value is not written again: every write takes the
        // ledger lock that waiting entries poll.
        if (recorded != Some(cores) || others.stripped) && record(ctx, id, cores, parent) {
            recorded = Some(cores);
        }
    }
}

/// What the ledger says of the other entries, for the sampler of `id`.
#[derive(Debug, Default, PartialEq)]
struct Others {
    /// The `task-traffic-controller` process of every other entry. Each
    /// is charged for what it runs, so where one is found below this
    /// entry's command its tree is left out: nothing is counted twice.
    /// That covers the entries nested in this one and also any that
    /// started below it as top-level ones (a stale lease in between).
    skip: HashSet<u32>,
    /// Some entry is nested in this one.
    nested: bool,
    /// This entry was registered with a parent the ledger no longer names.
    stripped: bool,
}

fn others(ledger: &Ledger, id: &str, parent: Option<&str>) -> Others {
    Others {
        skip: ledger
            .entries
            .iter()
            .filter(|e| e.id != id)
            .map(|e| e.pid)
            .collect(),
        nested: ledger
            .entries
            .iter()
            .any(|e| e.parent.as_deref() == Some(id)),
        stripped: parent.is_some()
            && ledger
                .find(id)
                .is_some_and(|e| e.parent.as_deref() != parent),
    }
}

/// Write `cores` to the entry, and `parent` when the ledger has lost it
/// (see [`crate::managed::try_admit_in`]); false when it could not be
/// written.
fn record(ctx: &Context, id: &str, cores: f64, parent: Option<&str>) -> bool {
    let Ok(Some(_guard)) = lock::try_ledger(ctx) else {
        return false;
    };
    let path = ctx.paths.ledger_file();
    let Ok(mut ledger) = Ledger::load(&path) else {
        return false;
    };
    let Some(e) = ledger.find_mut(id) else {
        return false;
    };
    e.using = Some(cores);
    if parent.is_some() && e.parent.as_deref() != parent {
        e.parent = parent.map(str::to_string);
    }
    ledger.save(&path).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(pid: u32, parent: Option<u32>, cpu: f32) -> Proc {
        Proc {
            pid,
            parent,
            cpu_percent: cpu,
            rss: u64::from(pid) << 20,
        }
    }

    #[test]
    fn sums_the_root_and_every_descendant_only() {
        let procs = [
            p(1, None, 50.0),
            p(10, Some(1), 100.0),
            p(11, Some(10), 250.0),
            p(12, Some(11), 50.0),
            p(20, Some(1), 400.0),
        ];
        let none = HashSet::new();
        assert!((tree_cores(&procs, 10, &none) - 4.0).abs() < 1e-9);
        assert!((tree_cores(&procs, 12, &none) - 0.5).abs() < 1e-9);
        assert_eq!(tree_cores(&procs, 99, &none), 0.0);
    }

    #[test]
    fn a_skipped_process_and_everything_below_it_is_left_out() {
        let procs = [
            p(10, None, 100.0),
            p(11, Some(10), 250.0),
            p(12, Some(11), 50.0),
            p(13, Some(10), 30.0),
        ];
        let skip = |pids: &[u32]| pids.iter().copied().collect::<HashSet<u32>>();
        assert!((tree_cores(&procs, 10, &skip(&[11])) - 1.3).abs() < 1e-9);
        assert!((tree_cores(&procs, 10, &skip(&[12, 13])) - 3.5).abs() < 1e-9);
        // The root is what is measured, whatever the ledger says of it.
        assert!((tree_cores(&procs, 10, &skip(&[10])) - 4.3).abs() < 1e-9);
        // The largest single process of what is left.
        assert_eq!(tree_peak_rss(&procs, 10, &skip(&[])), 13 << 20);
        assert_eq!(tree_peak_rss(&procs, 10, &skip(&[13])), 12 << 20);
        assert_eq!(tree_peak_rss(&procs, 10, &skip(&[11, 13])), 10 << 20);
        assert_eq!(tree_peak_rss(&procs, 99, &skip(&[])), 0);
    }

    fn entry(id: &str, pid: u32, parent: Option<&str>) -> crate::ledger::Entry {
        crate::ledger::Entry {
            id: id.into(),
            pid,
            state: crate::admit::State::Running,
            front: false,
            weight: 1,
            predicted_rss: 0,
            expected_secs: None,
            repo: String::new(),
            subdir: String::new(),
            argv: id.into(),
            key: id.into(),
            seq: u64::from(pid),
            enqueued_at: chrono::Utc::now(),
            started_at: None,
            using: None,
            backfilled: false,
            label: None,
            parent: parent.map(str::to_string),
            nested_cpu_secs: 0.0,
        }
    }

    #[test]
    fn every_other_entry_is_left_out_and_nesting_is_noticed() {
        let ledger = Ledger {
            entries: vec![
                entry("p", 1, None),
                entry("c1", 2, Some("p")),
                // Started below `p` with a stale lease: a top-level entry,
                // still measured on its own.
                entry("top", 4, None),
                entry("grandchild", 5, Some("c1")),
            ],
            ..Ledger::default()
        };
        let p = others(&ledger, "p", None);
        assert_eq!(p.skip, [2, 4, 5].into_iter().collect());
        assert!(p.nested && !p.stripped);
        let c1 = others(&ledger, "c1", Some("p"));
        assert_eq!(c1.skip, [1, 4, 5].into_iter().collect());
        assert!(c1.nested && !c1.stripped);
        let top = others(&ledger, "top", None);
        assert!(!top.nested && !top.stripped);
        // Registered as nested in `p`, but the ledger no longer says so.
        assert!(others(&ledger, "top", Some("p")).stripped);
        // An entry that is gone has nothing to put back.
        assert!(!others(&ledger, "gone", Some("p")).stripped);
    }

    #[test]
    fn the_sampler_puts_back_a_parent_the_ledger_lost() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = Context::new(crate::Paths::new(
            dir.path().join("state"),
            dir.path().join("config.toml"),
        ));
        let path = ctx.paths.ledger_file();
        let ledger = Ledger {
            entries: vec![entry("p", 1, None), entry("c", 2, None)],
            ..Ledger::default()
        };
        ledger.save(&path).unwrap();
        assert!(record(&ctx, "c", 1.5, Some("p")));
        let c = Ledger::load(&path).unwrap().find("c").cloned().unwrap();
        assert_eq!((c.using, c.parent.as_deref()), (Some(1.5), Some("p")));
        // A top-level entry stays one.
        assert!(record(&ctx, "p", 0.5, None));
        assert_eq!(Ledger::load(&path).unwrap().find("p").unwrap().parent, None);
        assert!(!record(&ctx, "gone", 1.0, None));
    }

    #[test]
    fn peak_covers_the_last_few_samples() {
        let mut p = RecentPeak::default();
        assert_eq!(p.push(4.0), 4.0);
        assert_eq!(p.push(0.5), 4.0);
        assert_eq!(p.push(1.0), 4.0);
        assert_eq!(p.push(2.0), 2.0);
        assert_eq!(p.push(0.0), 2.0);
    }

    #[test]
    fn cycles_and_negative_samples_are_harmless() {
        let procs = [
            p(1, Some(1), 100.0),
            p(2, Some(3), 100.0),
            p(3, Some(2), -5.0),
        ];
        let none = HashSet::new();
        assert!((tree_cores(&procs, 1, &none) - 1.0).abs() < 1e-9);
        assert!((tree_cores(&procs, 2, &none) - 1.0).abs() < 1e-9);
    }
}
