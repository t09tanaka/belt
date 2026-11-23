//! Who owns a capture that is not a run yet (spec 25.4 (b)).
//!
//! A submitting command writes one `capture_owner` row for its run before
//! anything else of the capture exists: before the run lock, the pending
//! root, an object, the plaintext secret bundle. The row carries the run ID,
//! the state (`capturing`, or `given_up` once the command knows it will not
//! submit), the pending root (written in the transaction that creates that
//! root) and the owner process (place, PID, start identity). Everything a
//! capture leaves behind can be named from the row alone, so a capture whose
//! owner gave up or is gone can be deleted, and one whose owner is alive is
//! never touched. Only the resident deletes a row.
use super::*;
use crate::ids::RunId;
use crate::platform::owned_temp::{OwnerRecord, OwnerState};

/// The table. An index of version 1 gets it on its next open; the version
/// stays 1 because nothing an older binary reads changed, and the index is
/// not part of what an upgrade rollback restores (spec 24.8).
pub(super) const TABLE: &str = "CREATE TABLE IF NOT EXISTS capture_owner(run_id TEXT PRIMARY KEY,state TEXT NOT NULL CHECK(state IN ('capturing','given_up')),root TEXT,place TEXT NOT NULL,pid INTEGER NOT NULL,start TEXT NOT NULL);";

/// A place at which a test child that makes a capture stops, says so and
/// waits to be killed (the kill points of spec 25.4 (b)). Not compiled into
/// the product.
#[cfg(test)]
pub(crate) fn stop_point(name: &str) {
    if std::env::var("BELT_TEST_CAPTURE_STOP").as_deref() == Ok(name) {
        crate::test_child::publish_ready().unwrap();
        loop {
            std::thread::park();
        }
    }
}
#[cfg(not(test))]
#[inline(always)]
pub(crate) fn stop_point(_name: &str) {}

/// What the resident may do with a capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureVerdict {
    /// `capturing`, and the owner is not proven gone here: never deleted.
    InProgress,
    /// `given_up`, or the owner is proven gone. Only `given_up` says that no
    /// enqueue of the capture is accepted: the enqueue of an owner that is
    /// gone may still be on its way.
    Abandoned,
    /// `capturing`, and the owner was recorded somewhere this process cannot
    /// see (another machine or PID namespace, or an unreadable record): kept
    /// and counted.
    Elsewhere,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureOwner {
    pub run: String,
    pub given_up: bool,
    /// The pending root of the capture, once it was created.
    pub root: Option<String>,
    pub(crate) owner: OwnerRecord,
}
impl CaptureOwner {
    pub fn verdict(&self) -> CaptureVerdict {
        if self.given_up {
            return CaptureVerdict::Abandoned;
        }
        match self.owner.state() {
            OwnerState::Present => CaptureVerdict::InProgress,
            OwnerState::Gone => CaptureVerdict::Abandoned,
            OwnerState::Elsewhere => CaptureVerdict::Elsewhere,
        }
    }
}

fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<CaptureOwner> {
    let state: String = r.get(1)?;
    Ok(CaptureOwner {
        run: r.get(0)?,
        given_up: state != "capturing",
        root: r.get(2)?,
        owner: OwnerRecord {
            place: r.get(3)?,
            // A PID that does not fit is a record that proves nothing.
            pid: u32::try_from(r.get::<_, i64>(4)?).unwrap_or(0),
            start: r.get(5)?,
        },
    })
}
const COLUMNS: &str = "run_id,state,root,place,pid,start";

impl Index {
    pub(super) fn ensure_capture_owner_table(&self, db: &mut Connection) -> Result<()> {
        let present: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='capture_owner')",
            [],
            |r| r.get(0),
        )?;
        if !present {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(TABLE)?;
            tx.commit()?;
        }
        Ok(())
    }
    /// The pending root of a capture and, for the capture of a run that has
    /// an owner row, its name in that row, in one transaction: there is no
    /// moment at which the root exists and the row does not name it. A
    /// capture without a row (a plan draft, whose owner is its directory)
    /// changes no row.
    ///
    /// The row names one root. While the root it names exists (an earlier
    /// capture of the run failed before its hand-off), a second root is
    /// refused and nothing is written: the earlier root would lose its only
    /// name, and a pending root nothing names is never deleted and holds
    /// every deletion of the store. The owner of the refused capture gives
    /// up, and the abandonment deletes the root the row still names.
    ///
    /// A capture this handle owns ([`SourceStore::own_capture`]) has a row,
    /// and its root is named by that row or is not created: the owner may
    /// have given up while the capture went on (an interrupt does not stop
    /// the blocking capture), and the abandonment may have deleted the row
    /// since. A root created then would have no name. So the row must still
    /// be `capturing` and take the name, or nothing is written.
    pub(crate) fn pending_of_capture(&self, id: &str, run: Option<&RunId>) -> Result<()> {
        let owned = run.is_some_and(|run| {
            self.owned
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains(run.as_str())
        });
        let mut c = self.connect_for_import()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(run) = run {
            let named: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM capture_owner o JOIN root r ON r.id=o.root WHERE o.run_id=?1)",
                [run.as_str()],
                |r| r.get(0),
            )?;
            ensure!(
                !named,
                "an earlier capture of this run left a pending root; the run is not captured again"
            );
        }
        tx.execute(
            "INSERT OR IGNORE INTO root(id,kind) VALUES(?1,'pending')",
            [id],
        )?;
        if let Some(run) = run {
            let named = tx.execute(
                "UPDATE capture_owner SET root=?1 WHERE run_id=?2 AND state='capturing'",
                params![id, run.as_str()],
            )?;
            // A row that is not `capturing` takes no name either, whoever
            // wrote it. Only a capture that never had a row (a plan draft)
            // goes on without one.
            let given_up: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM capture_owner WHERE run_id=?1)",
                [run.as_str()],
                |r| r.get(0),
            )?;
            // Dropping the transaction rolls the root back.
            ensure!(
                named == 1 || !(owned || given_up),
                "the capture of this run was given up; no pending root is created"
            );
        }
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn capture_owner(&self, run: &str) -> Result<Option<CaptureOwner>> {
        Ok(self
            .connect()?
            .query_row(
                &format!("SELECT {COLUMNS} FROM capture_owner WHERE run_id=?1"),
                [run],
                row,
            )
            .optional()?)
    }
    pub(crate) fn capture_owners(&self) -> Result<Vec<CaptureOwner>> {
        let c = self.connect()?;
        let mut s = c.prepare(&format!(
            "SELECT {COLUMNS} FROM capture_owner ORDER BY run_id"
        ))?;
        let rows = s.query_map([], row)?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }
}

/// The owner rows of the store of `state`, read without creating, locking or
/// changing anything (for `doctor`). `None` when there is no index.
pub fn inspect_owners(state: &Path) -> Result<Option<Vec<CaptureOwner>>> {
    let database = state.join("sources").join("retention.db");
    if !database.is_file() {
        return Ok(None);
    }
    let c = Connection::open_with_flags(
        database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    c.busy_timeout(std::time::Duration::from_secs(1))?;
    let present: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='capture_owner')",
        [],
        |r| r.get(0),
    )?;
    if !present {
        return Ok(Some(Vec::new()));
    }
    let mut s = c.prepare(&format!(
        "SELECT {COLUMNS} FROM capture_owner ORDER BY run_id"
    ))?;
    let rows = s.query_map([], row)?;
    Ok(Some(rows.collect::<std::result::Result<_, _>>()?))
}

impl SourceStore {
    /// The first durable write of a capture that will be submitted: this
    /// process owns the capture of `run`. Nothing of the capture may exist
    /// before this returns.
    pub fn own_capture(&self, run: &RunId) -> Result<()> {
        let owner = OwnerRecord::this_process()
            .context("this process cannot be recorded as the owner of a capture")?;
        self.own_capture_as(run, &owner)
    }
    /// A command that captures the same run again (the capture of a run is
    /// made once and reused) keeps its row. A run that another process owns,
    /// or that its owner gave up, is not taken over.
    pub(crate) fn own_capture_as(&self, run: &RunId, owner: &OwnerRecord) -> Result<()> {
        let mut c = self.retention.connect_for_import()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT OR IGNORE INTO capture_owner(run_id,state,root,place,pid,start) VALUES(?1,'capturing',NULL,?2,?3,?4)",
            params![run.as_str(), owner.place, i64::from(owner.pid), owner.start],
        )?;
        let mine: bool = tx.query_row(
            "SELECT state='capturing' AND place=?2 AND pid=?3 AND start=?4 FROM capture_owner WHERE run_id=?1",
            params![run.as_str(), owner.place, i64::from(owner.pid), owner.start],
            |r| r.get(0),
        )?;
        ensure!(
            mine,
            "the capture of this run has another owner, or was given up"
        );
        tx.commit()?;
        self.retention
            .owned
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(run.to_string());
        Ok(())
    }
    /// The owner will not submit this capture (its capture failed, its
    /// enqueue was refused, or it was interrupted). The row stays: only the
    /// resident, which owns deletion, removes it and what it names.
    pub fn give_up_capture(&self, run: &RunId) -> Result<()> {
        self.retention.connect_for_import()?.execute(
            "UPDATE capture_owner SET state='given_up' WHERE run_id=?1 AND state='capturing'",
            [run.as_str()],
        )?;
        Ok(())
    }
    pub fn capture_owner(&self, run: &RunId) -> Result<Option<CaptureOwner>> {
        self.retention.capture_owner(run.as_str())
    }
    pub fn capture_owners(&self) -> Result<Vec<CaptureOwner>> {
        self.retention.capture_owners()
    }
    /// The hand-over: the enqueue of `run` committed its row, so the capture
    /// is the run's. Resident only.
    pub(crate) fn release_capture(&self, run: &RunId) -> Result<()> {
        self.retention
            .connect()?
            .execute("DELETE FROM capture_owner WHERE run_id=?1", [run.as_str()])?;
        Ok(())
    }
}
