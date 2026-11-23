//! The pruning of runs (spec 25.5): the member that submitted a run removes
//! it from its own disk, whole, once the run has ended and its retention
//! has passed.
//!
//! One pass is one call of [`pass`], made by the result retention loop after
//! its page and in the same blocking job, so a page of the result collection
//! and the pruning never overlap (25.11). A pass continues the marked runs
//! first, then marks the runs that became due, then deletes the records that
//! belong to no run. It is bounded: 32 runs, 32 result paths (claims and
//! attempt directories), 32 input paths and 64 MiB of validation, each
//! shared by all the runs of the pass, and row transactions of 1,000 rows.
//!
//! Each run goes through the steps in the order of the spec's table. No step
//! needs its position: the sweep of the attempt directories and the roots
//! remember theirs in memory only to go on, and a pass starts every marked
//! run at step 2 and
//! each step finds what is left of its own work (a Vault bundle, a result
//! reference or claim, a root or a file of the source store, a row), so a
//! pass after a crash at any point does the rest and ends in the same state.
use super::*;
use crate::results::retention::Cursor;
use crate::results::store::{StoreError, LOCATOR_PREFIX};
use crate::secrets::vault::Vault;
use crate::source::retention::{AbandonCursor, PruneBudget};
use crate::source::SourceStore;
use crate::storage::{RowsState, StorageError, ROWS_PER_TRANSACTION};

/// Spec 25.11: the runs and the result paths of one pass.
const RUNS: usize = 32;
const PATHS: u32 = 32;
/// The row transactions of one pass: 64,000 rows. A run with more is
/// continued by the next pass.
const TRANSACTIONS: u32 = 64;
/// The attempts read at a time for the sweep of their result directories.
const ATTEMPTS: u32 = 256;

/// What a pass remembers for the next one. All of it can be lost: a new
/// process starts from the marks.
#[derive(Default)]
pub(super) struct Pruning {
    /// Where the page of marked runs continues, so that a run that cannot
    /// go on (a reader, a file that cannot be proven) does not keep the
    /// runs after it waiting.
    marked_after: Option<RunId>,
    /// Where the page of runs to judge continues.
    candidates_after: Option<RunId>,
    /// Where the sweep of the result directories of a marked run stands,
    /// so that a pass that reached its bound of paths continues it. After
    /// the mark nothing writes a result of the run, so one sweep under the
    /// exclusive pin is enough.
    sweeps: std::collections::BTreeMap<RunId, Sweep>,
    roots: AbandonCursor,
}

/// The sweep of the attempt directories of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Sweep {
    /// The last (task, attempt) whose directory went.
    After((String, String)),
    Done,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct PruneReport {
    pub clock_held: bool,
    pub marked: u32,
    /// Runs of which nothing is left.
    pub finished: u32,
    /// Marked runs a later pass continues.
    pub continuing: u32,
    /// Steps that failed, and files that could not be proven or deleted:
    /// kept, and tried again.
    pub unknown: u32,
    pub events: u32,
    pub timings: u32,
}

pub(super) struct Sample {
    pub wall: u64,
    pub mono: u64,
    pub boot: Option<String>,
}
impl Sample {
    pub(super) fn platform() -> Self {
        Self {
            wall: crate::platform::clock::wall_ms(),
            mono: crate::platform::clock::monotonic_ms(),
            boot: crate::platform::boot_id(),
        }
    }
}

struct Budget {
    /// Result paths (claims and attempt directories).
    paths: u32,
    transactions: u32,
    /// Input paths and validation bytes.
    inputs: PruneBudget,
}

#[cfg(test)]
thread_local! { static FAULT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) }; }
/// Stop the pass of this thread right after `stage` (a test of a resident
/// that is stopped there).
#[cfg(test)]
pub(super) fn set_fault(stage: Option<&'static str>) {
    FAULT.set(stage);
}
fn fault(stage: &str) -> anyhow::Result<()> {
    #[cfg(test)]
    if FAULT.get() == Some(stage) {
        anyhow::bail!("injected stop {stage}")
    }
    let _ = stage;
    Ok(())
}

/// One bounded pass. The caller holds the process owner for the whole call
/// and does not call it while a version is on trial.
#[allow(clippy::too_many_arguments)]
pub(super) fn pass(
    owner: &CoordinatorStore,
    files: &ResultStore,
    sources: &SourceStore,
    vault: &Vault,
    gc: &mut Cursor,
    state: &mut Pruning,
    retention_days: u32,
    sample: Sample,
) -> anyhow::Result<PruneReport> {
    let retention = crate::results::plan::retention_ms(retention_days);
    let mut report = PruneReport::default();
    // The marked runs come first: the pass right after a start continues
    // what the process before it began.
    let mut runs = owner.marked_runs(state.marked_after.take(), RUNS as u32)?;
    if runs.len() == RUNS {
        state.marked_after = runs.last().cloned();
    }
    // Step 1.
    let wall = sample.wall;
    let page = owner.mark_ended_runs(
        state.candidates_after.take(),
        RUNS as u32,
        sample.wall,
        sample.mono,
        sample.boot,
        retention,
    )?;
    state.candidates_after = page.next;
    report.clock_held = page.clock_held;
    report.marked = page.marked.len() as u32;
    fault("after_mark")?;
    for run in page.marked {
        if !runs.contains(&run) {
            runs.push(run);
        }
    }
    runs.truncate(RUNS);
    let mut budget = Budget {
        paths: PATHS,
        transactions: TRANSACTIONS,
        inputs: PruneBudget::pass(),
    };
    for run in &runs {
        match prune(
            owner,
            files,
            sources,
            vault,
            gc,
            state,
            &mut budget,
            &mut report,
            run,
            wall,
        ) {
            Ok(true) => report.finished += 1,
            Ok(false) => report.continuing += 1,
            // The run keeps its mark and what is left of it; the runs after
            // it are not held back, and the next pass tries again.
            Err(_error) => {
                // Never logged: the error can name a private path.
                #[cfg(test)]
                eprintln!("pruning of {run} stopped: {_error:#}");
                report.continuing += 1;
                report.unknown += 1;
            }
        }
    }
    if let Some(now) = page.now {
        (report.events, report.timings) =
            owner.prune_unowned(now, retention, ROWS_PER_TRANSACTION)?;
    }
    Ok(report)
}

/// Steps 2 to 8 for one marked run. `true` when nothing of the run is left.
#[allow(clippy::too_many_arguments)]
fn prune(
    owner: &CoordinatorStore,
    files: &ResultStore,
    sources: &SourceStore,
    vault: &Vault,
    gc: &mut Cursor,
    state: &mut Pruning,
    budget: &mut Budget,
    report: &mut PruneReport,
    run: &RunId,
    wall: u64,
) -> anyhow::Result<bool> {
    // 2. The secret bundle in the parent Vault.
    if !sources.prune_run_vault(owner, vault, run, &mut budget.inputs)?.done {
        return Ok(false);
    }
    fault("after_vault")?;
    // 3. The exclusive pin, without waiting: a reader that began before the
    // mark holds it shared, and this run waits for the next pass. A run
    // without a pin file has no reader. The pin is held to the end of the
    // pass and never across passes; the mark refuses every reader meanwhile.
    let pin = match owner.prune_pin(run) {
        Ok(pin) => pin,
        Err(StorageError::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    // The result collection must not come back to this run with a pin.
    gc.forget(run);
    if !results(owner, files, state, budget, report, run, wall)? {
        return Ok(false);
    }
    fault("after_results")?;
    // 4 and 5. The inputs, then the files and index rows named by the run.
    let inputs = sources.prune_run_inputs(owner, run, &mut state.roots, &mut budget.inputs)?;
    report.unknown += inputs.unknown;
    if !inputs.done {
        return Ok(false);
    }
    fault("after_inputs")?;
    // 6. The rows, children first.
    let mut ended = Vec::new();
    loop {
        if budget.transactions == 0 {
            return Ok(false);
        }
        budget.transactions -= 1;
        let page = owner.prune_rows(run, ROWS_PER_TRANSACTION)?;
        ended.extend(page.attempts);
        match page.state {
            RowsState::More => fault("during_rows")?,
            RowsState::Results => return Ok(false),
            RowsState::Done => break,
        }
    }
    fault("after_rows")?;
    // 7. After the commit that deleted the row of the run.
    release_attempts(ended);
    // 8. The pin file goes last. A stop before this line leaves a pin of a
    // run without a row, which the scan at the next start removes.
    gc.forget(run);
    state.sweeps.remove(run);
    if let Some(pin) = pin {
        owner.prune_unpin(pin)?;
    }
    Ok(true)
}

/// Step 3 for one run: every result reference is dropped and its file
/// claimed and deleted by the steps of the result collection (claim in the
/// database, delete the file, then forget the claim), without judging the
/// retention again. Then the directory of each attempt goes, with what an
/// unfinished transfer left in it. `false` when the pass's bound of paths
/// was reached or a file could not be deleted.
fn results(
    owner: &CoordinatorStore,
    files: &ResultStore,
    state: &mut Pruning,
    budget: &mut Budget,
    report: &mut PruneReport,
    run: &RunId,
    wall: u64,
) -> anyhow::Result<bool> {
    loop {
        if budget.paths == 0 {
            return Ok(false);
        }
        let (dropped, _foreign) = owner.prune_claim_results(run, budget.paths, wall)?;
        let pending = owner.prune_pending_results(run, budget.paths)?;
        if dropped == 0 && pending.is_empty() {
            break;
        }
        budget.paths = budget
            .paths
            .saturating_sub(dropped.max(pending.len() as u32));
        let mut held = false;
        for claim in &pending {
            anyhow::ensure!(
                ResultStore::owns(&claim.locator),
                "foreign result deletion claim"
            );
            match files.remove(&claim.locator) {
                Ok(()) => owner.prune_finish_result(claim)?,
                // It stays claimed with its data, and is tried again.
                Err(StoreError::Io(_)) => {
                    held = true;
                    report.unknown += 1;
                }
                Err(e) => return Err(e.into()),
            }
        }
        if held {
            return Ok(false);
        }
    }
    let mut after = match state.sweeps.get(run) {
        Some(Sweep::Done) => return Ok(true),
        Some(Sweep::After(after)) => after.clone(),
        None => (String::new(), String::new()),
    };
    // Forgetting a position costs a sweep again, never a directory.
    if state.sweeps.len() >= 2 * RUNS && !state.sweeps.contains_key(run) {
        state.sweeps.clear();
    }
    loop {
        if budget.paths == 0 {
            state.sweeps.insert(run.clone(), Sweep::After(after));
            return Ok(false);
        }
        let page = owner.run_attempts_page(run, after.clone(), budget.paths.min(ATTEMPTS))?;
        let full = page.len() as u32 == budget.paths.min(ATTEMPTS);
        for (task, attempt) in &page {
            // Each directory is one path of the pass's bound.
            budget.paths -= 1;
            match files.remove(&format!("{LOCATOR_PREFIX}{attempt}")) {
                Ok(()) => {}
                Err(StoreError::Io(_)) => {
                    report.unknown += 1;
                    state.sweeps.insert(run.clone(), Sweep::After(after));
                    return Ok(false);
                }
                Err(e) => return Err(e.into()),
            }
            after = (task.clone(), attempt.to_string());
        }
        if !full {
            break;
        }
    }
    state.sweeps.insert(run.clone(), Sweep::Done);
    Ok(true)
}

/// Step 7 (spec 25.5): the attempts of a pruned run that a worker accepted
/// and that were not acknowledged are released on their workers. The
/// operation (`release_attempt`, 25.6) comes with stage 3; until then
/// nothing is sent, and the worker deletes its copy after its own wait, as
/// before. The list is exactly what the transactions of step 6 read before
/// they deleted the attempt rows; stage 3 selects from it and sends here.
fn release_attempts(attempts: Vec<(WorkerId, AttemptId)>) {
    drop(attempts);
}

#[cfg(test)]
mod tests;
