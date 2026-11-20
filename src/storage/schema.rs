//! Schema migrations. Step `i` takes a database from version `i` to `i + 1`
//! (the first step also runs on an empty database). Append a step; never edit
//! a released one.

use super::policy::CompatTable;
use super::DbKind;

/// Tables every database shares: the append-only event journal and the
/// references GC works on.
const COMMON: &str = "
CREATE TABLE event (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       TEXT,
    attempt_id   TEXT,
    -- 1, 2, 3, ... per attempt with no holes (NULL for an event of no
    -- attempt): the sequence a watcher of one attempt follows. Numbered by
    -- the INSERT itself, in the writer's transaction.
    attempt_seq  INTEGER CHECK ((attempt_id IS NULL) = (attempt_seq IS NULL)),
    kind         TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    at_ms        INTEGER NOT NULL
);
CREATE INDEX event_attempt ON event(attempt_id, seq);
CREATE UNIQUE INDEX event_attempt_seq ON event(attempt_id, attempt_seq)
    WHERE attempt_id IS NOT NULL;
CREATE TRIGGER event_no_update BEFORE UPDATE ON event
BEGIN SELECT RAISE(ABORT, 'event journal is append-only'); END;
CREATE TRIGGER event_no_delete BEFORE DELETE ON event
BEGIN SELECT RAISE(ABORT, 'event journal is append-only'); END;
-- A file whose last reference GC dropped. The row is written in the same
-- transaction that drops the reference, and removed only after the file is
-- gone; add_blob_ref refuses a locator listed here, so a new reference can
-- never point at a file that is about to be deleted.
CREATE TABLE pending_delete (
    locator      TEXT PRIMARY KEY,
    queued_at_ms INTEGER NOT NULL
);
";

const COORDINATOR_V1: &str = "
CREATE TABLE cluster (
    cluster_id     TEXT PRIMARY KEY,
    coordinator_id TEXT NOT NULL,
    created_at_ms  INTEGER NOT NULL
);
CREATE TABLE coordinator_state (
    singleton  INTEGER PRIMARY KEY CHECK (singleton = 1),
    cluster_id TEXT NOT NULL REFERENCES cluster(cluster_id),
    epoch      INTEGER NOT NULL
);
CREATE TABLE source_ref (
    source_id       TEXT PRIMARY KEY,
    manifest_digest TEXT NOT NULL,
    created_at_ms   INTEGER NOT NULL
);
CREATE TABLE secret_bundle_ref (
    bundle_id     TEXT PRIMARY KEY,
    digest        TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE run (
    run_id           TEXT PRIMARY KEY,
    source_id        TEXT REFERENCES source_ref(source_id),
    secret_bundle_id TEXT REFERENCES secret_bundle_ref(bundle_id),
    spec_json        TEXT NOT NULL,
    created_at_ms    INTEGER NOT NULL,
    closed_at_ms     INTEGER
);
CREATE TABLE task (
    task_id   TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES run(run_id),
    spec_json     TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE attempt (
    attempt_id             TEXT PRIMARY KEY,
    task_id            TEXT NOT NULL REFERENCES task(task_id),
    worker_id              TEXT NOT NULL,
    generation             INTEGER NOT NULL,
    payload_hash           TEXT NOT NULL,
    coordinator_epoch      INTEGER NOT NULL,
    permit_id              TEXT,
    state                  TEXT NOT NULL CHECK (state IN
        ('accepted','preparing','ready','starting','active','cancelling','finalizing','terminal')),
    termination_json       TEXT,
    receipts_json          TEXT NOT NULL,
    observation            TEXT NOT NULL DEFAULT 'known'
        CHECK (observation IN ('known','reconciling','unknown')),
    cancel_requested_at_ms INTEGER,
    retired                INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1)),
    -- When the retire was confirmed: where GC measures retention from for an
    -- attempt that never became terminal.
    retired_at_ms          INTEGER,
    -- The coordinator gave up collecting the outputs: GC no longer waits.
    collection_failed_at_ms INTEGER,
    created_at_ms          INTEGER NOT NULL,
    terminal_at_ms         INTEGER,
    CHECK ((state = 'terminal') = (termination_json IS NOT NULL)),
    CHECK ((state = 'terminal') = (terminal_at_ms IS NOT NULL)),
    CHECK ((retired = 1) = (retired_at_ms IS NOT NULL)),
    UNIQUE (task_id, generation)
);
-- At most one attempt per task that is neither terminal nor confirmed retired.
CREATE UNIQUE INDEX attempt_one_live ON attempt(task_id)
    WHERE state <> 'terminal' AND retired = 0;
CREATE INDEX attempt_worker ON attempt(worker_id);
CREATE TABLE placement (
    placement_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id   TEXT NOT NULL REFERENCES task(task_id),
    worker_id     TEXT NOT NULL,
    attempt_id    TEXT REFERENCES attempt(attempt_id),
    demand_json   TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    acked_at_ms   INTEGER
);
-- Kept for the life of the cluster: it is what stops an old submit from running.
CREATE TABLE retire_tombstone (
    attempt_id       TEXT PRIMARY KEY REFERENCES attempt(attempt_id),
    worker_id        TEXT NOT NULL,
    reason           TEXT NOT NULL,
    requested_at_ms  INTEGER NOT NULL,
    confirmed_at_ms  INTEGER
);
CREATE TRIGGER retire_tombstone_no_delete BEFORE DELETE ON retire_tombstone
BEGIN SELECT RAISE(ABORT, 'retire tombstones are kept for the life of the cluster'); END;
-- Only the confirmation may be added; what was asked for never changes.
CREATE TRIGGER retire_tombstone_fixed BEFORE UPDATE ON retire_tombstone
WHEN NEW.attempt_id <> OLD.attempt_id OR NEW.worker_id <> OLD.worker_id
    OR NEW.reason <> OLD.reason OR NEW.requested_at_ms <> OLD.requested_at_ms
    OR (OLD.confirmed_at_ms IS NOT NULL AND NEW.confirmed_at_ms IS NOT OLD.confirmed_at_ms)
BEGIN SELECT RAISE(ABORT, 'a retire tombstone only gains its confirmation'); END;
CREATE TABLE matrix_group (
    group_id      TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES run(run_id),
    max_parallel  INTEGER NOT NULL CHECK (max_parallel > 0),
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE matrix_permit (
    permit_id      TEXT PRIMARY KEY,
    group_id       TEXT NOT NULL REFERENCES matrix_group(group_id),
    attempt_id     TEXT UNIQUE REFERENCES attempt(attempt_id),
    held           INTEGER NOT NULL DEFAULT 1 CHECK (held IN (0, 1)),
    created_at_ms  INTEGER NOT NULL,
    released_at_ms INTEGER
);
CREATE TABLE artifact_receipt (
    attempt_id          TEXT NOT NULL REFERENCES attempt(attempt_id),
    name                TEXT NOT NULL,
    required            INTEGER NOT NULL CHECK (required IN (0, 1)),
    rel_path            TEXT,
    size                INTEGER,
    digest              TEXT,
    source_id           TEXT,
    plan_digest         TEXT,
    profile_fingerprint TEXT,
    persisted_at_ms     INTEGER,
    PRIMARY KEY (attempt_id, name)
);
CREATE TABLE blob_ref (
    ref_id        TEXT PRIMARY KEY,
    attempt_id    TEXT NOT NULL REFERENCES attempt(attempt_id),
    kind          TEXT NOT NULL CHECK (kind IN ('input','log','result')),
    locator       TEXT NOT NULL,
    pins          INTEGER NOT NULL DEFAULT 0 CHECK (pins >= 0),
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX blob_ref_attempt ON blob_ref(attempt_id);
CREATE INDEX blob_ref_locator ON blob_ref(locator);
";

/// The queue: a cancel of a task is durable before any attempt of it exists,
/// and a cancelled task is never placed.
const COORDINATOR_V2: &str = "
ALTER TABLE task ADD COLUMN cancel_requested_at_ms INTEGER;
CREATE INDEX placement_attempt ON placement(attempt_id);
";

/// Result collection: what was read from the worker, by digest, and how the
/// collection of an ended attempt was closed.
const COORDINATOR_V3: &str = "
CREATE TABLE collection (
    attempt_id    TEXT PRIMARY KEY REFERENCES attempt(attempt_id),
    result_digest TEXT,
    result_json   TEXT,
    logs_json     TEXT,
    verdict       TEXT CHECK (verdict IN ('complete','required_missing','gave_up')),
    acked_at_ms   INTEGER,
    closed_at_ms  INTEGER,
    strikes       INTEGER NOT NULL DEFAULT 0 CHECK (strikes >= 0),
    updated_at_ms INTEGER NOT NULL,
    CHECK ((result_digest IS NULL) = (result_json IS NULL)),
    CHECK (acked_at_ms IS NULL OR closed_at_ms IS NOT NULL),
    CHECK ((closed_at_ms IS NULL) = (verdict IS NULL))
);
";

const WORKER_V1: &str = "
CREATE TABLE worker_state (
    singleton             INTEGER PRIMARY KEY CHECK (singleton = 1),
    worker_id             TEXT NOT NULL,
    agent_epoch           INTEGER NOT NULL,
    max_coordinator_epoch INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE worker_attempt (
    attempt_id             TEXT PRIMARY KEY,
    run_id                 TEXT NOT NULL,
    task_id            TEXT NOT NULL,
    generation             INTEGER NOT NULL,
    payload_hash           TEXT NOT NULL,
    coordinator_epoch      INTEGER NOT NULL,
    spec_json              TEXT NOT NULL,
    state                  TEXT NOT NULL CHECK (state IN
        ('accepted','preparing','ready','starting','active','cancelling','finalizing','terminal')),
    termination_json       TEXT,
    receipts_json          TEXT NOT NULL,
    cancel_requested_at_ms INTEGER,
    retired                INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1)),
    accepted_at_ms         INTEGER NOT NULL,
    terminal_at_ms         INTEGER,
    result_json            TEXT,
    acked_at_ms            INTEGER,
    CHECK ((state = 'terminal') = (termination_json IS NOT NULL)),
    CHECK ((state = 'terminal') = (terminal_at_ms IS NOT NULL))
);
-- A retired attempt still counts until it is terminal (confirmed stopped).
CREATE UNIQUE INDEX worker_attempt_one_live ON worker_attempt(task_id)
    WHERE state <> 'terminal';
-- A cancel or retire that arrived before (or without) the submit. Never deleted.
CREATE TABLE tombstone (
    attempt_id        TEXT PRIMARY KEY,
    kind              TEXT NOT NULL CHECK (kind IN ('cancel','retire')),
    coordinator_epoch INTEGER NOT NULL,
    recorded_at_ms    INTEGER NOT NULL
);
CREATE TRIGGER tombstone_no_delete BEFORE DELETE ON tombstone
BEGIN SELECT RAISE(ABORT, 'tombstones are kept for the life of the cluster'); END;
-- A cancel may become a retire; the attempt and the epoch it was recorded
-- under never change, and a retire never goes back to a cancel.
CREATE TRIGGER tombstone_fixed BEFORE UPDATE ON tombstone
WHEN NEW.attempt_id <> OLD.attempt_id OR NEW.coordinator_epoch <> OLD.coordinator_epoch
    OR NEW.recorded_at_ms <> OLD.recorded_at_ms
    OR (OLD.kind = 'retire' AND NEW.kind <> 'retire')
BEGIN SELECT RAISE(ABORT, 'a tombstone keeps its attempt, epoch and time; a retire stays a retire'); END;
CREATE TABLE command_launch (
    attempt_id       TEXT NOT NULL REFERENCES worker_attempt(attempt_id),
    node_id          TEXT NOT NULL,
    intent_at_ms     INTEGER NOT NULL,
    boot_id          TEXT,
    pid              INTEGER,
    start_identity   TEXT,
    identity_at_ms   INTEGER,
    PRIMARY KEY (attempt_id, node_id)
);
CREATE TABLE lease (
    attempt_id        TEXT PRIMARY KEY REFERENCES worker_attempt(attempt_id),
    coordinator_epoch INTEGER NOT NULL,
    lease_seq         INTEGER NOT NULL,
    renewed_at_ms     INTEGER NOT NULL,
    expired_at_ms     INTEGER
);
CREATE TABLE log_spool (
    attempt_id    TEXT NOT NULL REFERENCES worker_attempt(attempt_id),
    stream        TEXT NOT NULL,
    written_bytes INTEGER NOT NULL DEFAULT 0,
    dropped_bytes INTEGER NOT NULL DEFAULT 0,
    last_seq      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (attempt_id, stream)
);
CREATE TABLE blob_ref (
    ref_id        TEXT PRIMARY KEY,
    attempt_id    TEXT NOT NULL REFERENCES worker_attempt(attempt_id),
    kind          TEXT NOT NULL CHECK (kind IN ('input','log','result')),
    locator       TEXT NOT NULL,
    pins          INTEGER NOT NULL DEFAULT 0 CHECK (pins >= 0),
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX blob_ref_attempt ON blob_ref(attempt_id);
CREATE INDEX blob_ref_locator ON blob_ref(locator);
";

/// Worker step 4 (spec chapter 24): every attempt-keyed table is rebuilt with
/// the submitting member in its key, so one member cannot retire, conflict
/// with or read another member's work. The rows of the old tables are not
/// carried over: nothing says which member owned them. The step runs only
/// when no attempt is live and no result waits to be collected
/// ([`precheck`]); the files the dropped references named are queued for
/// deletion in the same transaction, and so are the ids of the dropped
/// attempts, whose directories the agent removes at its next start.
const WORKER_V4: &str = "
INSERT OR IGNORE INTO pending_delete (locator, queued_at_ms) SELECT DISTINCT locator, 0 FROM blob_ref;
-- The attempts this step dropped. Their directories (spec, supervisor
-- state, workspace record) have no row to be cleaned up by any more: the
-- agent removes each and then its entry here.
CREATE TABLE dropped_attempt (
    attempt_id TEXT PRIMARY KEY
) WITHOUT ROWID;
INSERT INTO dropped_attempt (attempt_id) SELECT attempt_id FROM worker_attempt;
DROP TRIGGER tombstone_no_delete;
DROP TRIGGER tombstone_fixed;
DROP TABLE blob_ref;
DROP TABLE log_spool;
DROP TABLE lease;
DROP TABLE command_launch;
DROP TABLE tombstone;
DROP TABLE worker_attempt;
ALTER TABLE worker_state DROP COLUMN max_coordinator_epoch;
-- The highest epoch each submitting member has shown. One member's epoch
-- says nothing about another's.
CREATE TABLE member_epoch (
    coordinator_id TEXT PRIMARY KEY,
    max_epoch      INTEGER NOT NULL CHECK (max_epoch >= 0)
) WITHOUT ROWID;
CREATE TABLE worker_attempt (
    coordinator_id         TEXT NOT NULL,
    attempt_id             TEXT NOT NULL,
    run_id                 TEXT NOT NULL,
    task_id                TEXT NOT NULL,
    generation             INTEGER NOT NULL,
    payload_hash           TEXT NOT NULL,
    coordinator_epoch      INTEGER NOT NULL,
    spec_json              TEXT NOT NULL,
    state                  TEXT NOT NULL CHECK (state IN
        ('accepted','preparing','ready','starting','active','cancelling','finalizing','terminal')),
    termination_json       TEXT,
    receipts_json          TEXT NOT NULL,
    cancel_requested_at_ms INTEGER,
    retired                INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1)),
    accepted_at_ms         INTEGER NOT NULL,
    terminal_at_ms         INTEGER,
    result_json            TEXT,
    acked_at_ms            INTEGER,
    PRIMARY KEY (coordinator_id, attempt_id),
    CHECK ((state = 'terminal') = (termination_json IS NOT NULL)),
    CHECK ((state = 'terminal') = (terminal_at_ms IS NOT NULL))
);
-- A retired attempt still counts until it is terminal (confirmed stopped).
CREATE UNIQUE INDEX worker_attempt_one_live ON worker_attempt(coordinator_id, task_id)
    WHERE state <> 'terminal';
CREATE INDEX worker_attempt_live ON worker_attempt(accepted_at_ms) WHERE state <> 'terminal';
-- A cancel or retire. `delivered` is 1 when the attempt itself was accepted
-- here; such a tombstone is never deleted. One whose attempt never arrived
-- may go once its member's epoch has moved on (a late submit under the old
-- epoch is refused as stale).
CREATE TABLE tombstone (
    coordinator_id    TEXT NOT NULL,
    attempt_id        TEXT NOT NULL,
    kind              TEXT NOT NULL CHECK (kind IN ('cancel','retire')),
    coordinator_epoch INTEGER NOT NULL,
    recorded_at_ms    INTEGER NOT NULL,
    delivered         INTEGER NOT NULL DEFAULT 0 CHECK (delivered IN (0, 1)),
    PRIMARY KEY (coordinator_id, attempt_id)
);
CREATE INDEX tombstone_undelivered ON tombstone(coordinator_id, coordinator_epoch) WHERE delivered = 0;
CREATE TRIGGER tombstone_no_delete BEFORE DELETE ON tombstone WHEN OLD.delivered = 1
BEGIN SELECT RAISE(ABORT, 'the tombstone of an accepted attempt is kept for the life of the group'); END;
CREATE TRIGGER tombstone_fixed BEFORE UPDATE ON tombstone
WHEN NEW.coordinator_id <> OLD.coordinator_id OR NEW.attempt_id <> OLD.attempt_id
    OR NEW.coordinator_epoch <> OLD.coordinator_epoch
    OR NEW.recorded_at_ms <> OLD.recorded_at_ms
    OR NEW.delivered <> OLD.delivered
    OR (OLD.kind = 'retire' AND NEW.kind <> 'retire')
BEGIN SELECT RAISE(ABORT, 'a tombstone keeps its member, attempt, epoch and time; a retire stays a retire'); END;
CREATE TABLE command_launch (
    coordinator_id   TEXT NOT NULL,
    attempt_id       TEXT NOT NULL,
    node_id          TEXT NOT NULL,
    intent_at_ms     INTEGER NOT NULL,
    boot_id          TEXT,
    pid              INTEGER,
    start_identity   TEXT,
    identity_at_ms   INTEGER,
    PRIMARY KEY (coordinator_id, attempt_id, node_id),
    FOREIGN KEY (coordinator_id, attempt_id) REFERENCES worker_attempt(coordinator_id, attempt_id)
);
CREATE TABLE lease (
    coordinator_id    TEXT NOT NULL,
    attempt_id        TEXT NOT NULL,
    coordinator_epoch INTEGER NOT NULL,
    lease_seq         INTEGER NOT NULL,
    renewed_at_ms     INTEGER NOT NULL,
    expired_at_ms     INTEGER,
    PRIMARY KEY (coordinator_id, attempt_id),
    FOREIGN KEY (coordinator_id, attempt_id) REFERENCES worker_attempt(coordinator_id, attempt_id)
);
CREATE TABLE log_spool (
    coordinator_id TEXT NOT NULL,
    attempt_id     TEXT NOT NULL,
    stream         TEXT NOT NULL,
    written_bytes  INTEGER NOT NULL DEFAULT 0,
    dropped_bytes  INTEGER NOT NULL DEFAULT 0,
    last_seq       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (coordinator_id, attempt_id, stream),
    FOREIGN KEY (coordinator_id, attempt_id) REFERENCES worker_attempt(coordinator_id, attempt_id)
);
CREATE TABLE blob_ref (
    ref_id         TEXT PRIMARY KEY,
    coordinator_id TEXT NOT NULL,
    attempt_id     TEXT NOT NULL,
    kind           TEXT NOT NULL CHECK (kind IN ('input','log','result')),
    locator        TEXT NOT NULL,
    pins           INTEGER NOT NULL DEFAULT 0 CHECK (pins >= 0),
    created_at_ms  INTEGER NOT NULL,
    FOREIGN KEY (coordinator_id, attempt_id) REFERENCES worker_attempt(coordinator_id, attempt_id)
);
CREATE INDEX blob_ref_attempt ON blob_ref(coordinator_id, attempt_id);
CREATE INDEX blob_ref_locator ON blob_ref(locator);
-- The journal keeps its sequence; an event of an attempt names its member
-- from here on. Events of the dropped attempts have none and are not shown
-- to anyone.
ALTER TABLE event ADD COLUMN coordinator_id TEXT;
DROP INDEX event_attempt;
DROP INDEX event_attempt_seq;
CREATE INDEX event_attempt ON event(coordinator_id, attempt_id, seq);
CREATE UNIQUE INDEX event_attempt_seq ON event(coordinator_id, attempt_id, attempt_seq)
    WHERE attempt_id IS NOT NULL AND coordinator_id IS NOT NULL;
-- The generations of the group key this worker holds: the one it signs with
-- and, during a rotation, the next one. Number and fingerprint only; the key
-- bytes stay in their files.
CREATE TABLE group_key (
    slot        TEXT PRIMARY KEY CHECK (slot IN ('current','next')),
    generation  INTEGER NOT NULL CHECK (generation > 0),
    fingerprint TEXT NOT NULL
) WITHOUT ROWID;
-- A member's name and its coordinator id, fixed at the first proof.
CREATE TABLE group_member (
    name           TEXT PRIMARY KEY,
    coordinator_id TEXT NOT NULL UNIQUE,
    pinned_at_ms   INTEGER NOT NULL
) WITHOUT ROWID;
-- The second half of this worker's identity; rebuilt when the key is sent again.
CREATE TABLE worker_identity_tail (
    singleton     INTEGER PRIMARY KEY CHECK (singleton = 1),
    tail          TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
-- A member told this worker that it retired this identity.
CREATE TABLE retired_identity (
    coordinator_id TEXT NOT NULL,
    worker_boot_id TEXT NOT NULL,
    recorded_at_ms INTEGER NOT NULL,
    PRIMARY KEY (coordinator_id, worker_boot_id)
) WITHOUT ROWID;
-- The hold a self-update places (not a drain; `resume` does not lift it).
CREATE TABLE upgrade_hold (
    singleton  INTEGER PRIMARY KEY CHECK (singleton = 1),
    state_json TEXT NOT NULL,
    set_at_ms  INTEGER NOT NULL
);
INSERT INTO schema_meta(key,value) VALUES('group_membership_v1','1');
";

/// What must hold before step `to` of a `kind` database runs, checked in the
/// step's own transaction. Worker step 4 drops every attempt row, so it is
/// refused while one is live, and while one has ended with a result nobody
/// collected (the result would be gone and its task would stay unknown to
/// the member that submitted it); the database is left as it was.
pub(crate) fn precheck(
    kind: &str,
    to: u32,
    conn: &rusqlite::Connection,
) -> Result<(), super::StorageError> {
    if kind != "worker" || to != 4 {
        return Ok(());
    }
    let live: i64 = conn.query_row(
        "SELECT count(*) FROM worker_attempt WHERE state <> 'terminal'",
        [],
        |r| r.get(0),
    )?;
    let uncollected: i64 = conn.query_row(
        "SELECT count(*) FROM worker_attempt WHERE state = 'terminal' AND acked_at_ms IS NULL",
        [],
        |r| r.get(0),
    )?;
    if live == 0 && uncollected == 0 {
        return Ok(());
    }
    Err(super::StorageError::MigrationRefused(format!(
        "{live} attempt(s) on this worker are still live and {uncollected} ended attempt(s) \
         have a result that was not collected yet, and this version rebuilds the worker \
         database without carrying attempts over. Start the previous version again, run \
         `belt workers drain` for this worker, wait until its attempts have ended and their \
         results are collected (the coordinator that submitted them must run and reach this \
         worker), then start this version"
    )))
}

/// The migration list of a database kind.
pub(crate) fn steps(kind: DbKind) -> Vec<String> {
    match kind {
        DbKind::Coordinator => vec![
            format!("{COMMON}{COORDINATOR_V1}"),
            COORDINATOR_V2.to_string(),
            COORDINATOR_V3.to_string(),
            "CREATE INDEX task_run_key ON task(run_id, task_id);".to_string(),
            r#"CREATE TABLE run_cancel_intent (run_id TEXT PRIMARY KEY, requested_at_ms INTEGER NOT NULL);
CREATE TRIGGER task_inherit_cancel AFTER INSERT ON task WHEN EXISTS(SELECT 1 FROM run_cancel_intent WHERE run_id=NEW.run_id)
BEGIN UPDATE task SET cancel_requested_at_ms=(SELECT requested_at_ms FROM run_cancel_intent WHERE run_id=NEW.run_id) WHERE task_id=NEW.task_id; END;
CREATE TRIGGER task_keep_cancel AFTER UPDATE OF cancel_requested_at_ms ON task WHEN NEW.cancel_requested_at_ms IS NULL AND EXISTS(SELECT 1 FROM run_cancel_intent WHERE run_id=NEW.run_id)
BEGIN UPDATE task SET cancel_requested_at_ms=(SELECT requested_at_ms FROM run_cancel_intent WHERE run_id=NEW.run_id) WHERE task_id=NEW.task_id; END;
CREATE TRIGGER attempt_cancel_gate BEFORE INSERT ON attempt WHEN EXISTS(SELECT 1 FROM task WHERE task_id=NEW.task_id AND cancel_requested_at_ms IS NOT NULL)
BEGIN SELECT RAISE(ABORT,'cancelled task cannot create an attempt'); END;"#.to_string(),
            r#"ALTER TABLE run ADD COLUMN control_revision INTEGER NOT NULL DEFAULT 0 CHECK(control_revision >= 0);
ALTER TABLE task ADD COLUMN min_generation INTEGER NOT NULL DEFAULT 1 CHECK(min_generation > 0);
ALTER TABLE task ADD COLUMN authorization_revision INTEGER NOT NULL DEFAULT 0 CHECK(authorization_revision >= 0);
ALTER TABLE run_cancel_intent ADD COLUMN control_revision INTEGER NOT NULL DEFAULT 0 CHECK(control_revision >= 0);
CREATE TABLE control_request(request_id TEXT PRIMARY KEY, operation TEXT NOT NULL CHECK(operation IN ('cancel','retry')), target_json TEXT NOT NULL, expected_revision INTEGER NOT NULL, reply_json TEXT NOT NULL);
CREATE TRIGGER run_inherit_control_revision AFTER INSERT ON run WHEN EXISTS(SELECT 1 FROM run_cancel_intent WHERE run_id=NEW.run_id)
BEGIN UPDATE run SET control_revision=MAX(control_revision,(SELECT control_revision FROM run_cancel_intent WHERE run_id=NEW.run_id)) WHERE run_id=NEW.run_id; END;
DROP TRIGGER task_inherit_cancel;
DROP TRIGGER task_keep_cancel;
CREATE TRIGGER task_inherit_cancel AFTER INSERT ON task WHEN EXISTS(SELECT 1 FROM run_cancel_intent WHERE run_id=NEW.run_id AND control_revision>=NEW.authorization_revision)
BEGIN UPDATE task SET cancel_requested_at_ms=(SELECT requested_at_ms FROM run_cancel_intent WHERE run_id=NEW.run_id) WHERE task_id=NEW.task_id; END;
CREATE TRIGGER task_keep_cancel AFTER UPDATE OF cancel_requested_at_ms ON task WHEN NEW.cancel_requested_at_ms IS NULL AND EXISTS(SELECT 1 FROM run_cancel_intent WHERE run_id=NEW.run_id AND control_revision>=NEW.authorization_revision)
BEGIN UPDATE task SET cancel_requested_at_ms=(SELECT requested_at_ms FROM run_cancel_intent WHERE run_id=NEW.run_id) WHERE task_id=NEW.task_id; END;"#.to_string(),
            r#"CREATE TABLE collection_log (
    attempt_id TEXT NOT NULL REFERENCES attempt(attempt_id),
    stream TEXT NOT NULL CHECK(stream IN ('stdout','stderr')),
    committed_bytes INTEGER NOT NULL DEFAULT 0 CHECK(committed_bytes >= 0),
    eof INTEGER NOT NULL DEFAULT 0 CHECK(eof IN (0,1)),
    log_truncated INTEGER NOT NULL DEFAULT 0 CHECK(log_truncated IN (0,1)),
    dropped_bytes INTEGER NOT NULL DEFAULT 0 CHECK(dropped_bytes >= 0),
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY(attempt_id,stream)
);"#.to_string(),
            r#"CREATE TABLE result_gc_ref (
    ref_id TEXT PRIMARY KEY REFERENCES blob_ref(ref_id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES run(run_id)
);
CREATE INDEX result_gc_ref_run ON result_gc_ref(run_id,ref_id);
INSERT INTO result_gc_ref(ref_id,run_id)
    SELECT b.ref_id,t.run_id FROM blob_ref b JOIN attempt a USING(attempt_id) JOIN task t USING(task_id) WHERE b.kind IN ('log','result');
CREATE TRIGGER result_gc_ref_insert AFTER INSERT ON blob_ref WHEN NEW.kind IN ('log','result')
BEGIN INSERT INTO result_gc_ref(ref_id,run_id) SELECT NEW.ref_id,t.run_id FROM attempt a JOIN task t USING(task_id) WHERE a.attempt_id=NEW.attempt_id; END;
CREATE TABLE result_delete_claim (
    locator TEXT PRIMARY KEY REFERENCES pending_delete(locator),
    run_id TEXT NOT NULL REFERENCES run(run_id),
    attempt_id TEXT NOT NULL REFERENCES attempt(attempt_id)
);
CREATE INDEX result_delete_claim_run ON result_delete_claim(run_id,locator);
CREATE TABLE result_gc_clock(singleton INTEGER PRIMARY KEY CHECK(singleton=1), state_json TEXT NOT NULL);
CREATE INDEX attempt_task_key ON attempt(task_id,attempt_id);"#.to_string(),
            r#"CREATE INDEX event_task_waiting ON event(json_extract(payload_json,'$.task'),seq DESC) WHERE kind='task_waiting';
INSERT INTO schema_meta(key,value) VALUES('execution_requirements_v1','1');"#.to_string(),
            "INSERT INTO schema_meta(key,value) VALUES('frozen_toolchains_v1','1');".into(),
            // Bounded reads: the paged walks seek instead of scanning a
            // worker's whole history. Performance only; it adds no feature
            // marker, the version bump alone makes an older binary refuse.
            "CREATE INDEX placement_unacked ON placement(worker_id,placement_id) WHERE acked_at_ms IS NULL;\nCREATE INDEX attempt_worker_live ON attempt(worker_id,created_at_ms) WHERE state <> 'terminal' AND retired=0;".into(),
            // The identity an ephemeral worker reported last, and the
            // identities that were retired (`workers forget`, or a new start
            // under the same name). A retired identity is kept for the life
            // of the cluster: it is what refuses a worker that comes back
            // late.
            r#"CREATE TABLE worker_identity (
    worker_id        TEXT PRIMARY KEY,
    boot_id          TEXT NOT NULL,
    first_seen_at_ms INTEGER NOT NULL
);
CREATE TABLE worker_identity_tombstone (
    worker_id      TEXT NOT NULL,
    boot_id        TEXT NOT NULL,
    reason         TEXT NOT NULL CHECK (reason IN ('forgotten','replaced')),
    recorded_at_ms INTEGER NOT NULL,
    PRIMARY KEY (worker_id, boot_id)
);
CREATE TRIGGER worker_identity_tombstone_no_delete BEFORE DELETE ON worker_identity_tombstone
BEGIN SELECT RAISE(ABORT, 'identity tombstones are kept for the life of the cluster'); END;
CREATE TRIGGER worker_identity_tombstone_fixed BEFORE UPDATE ON worker_identity_tombstone
BEGIN SELECT RAISE(ABORT, 'an identity tombstone never changes'); END;"#
                .to_string(),
            r#"CREATE INDEX matrix_permit_held ON matrix_permit(group_id) WHERE held=1;
CREATE TRIGGER matrix_attempt_ended AFTER UPDATE OF state,retired ON attempt
WHEN NEW.permit_id IS NOT NULL AND (NEW.state='terminal' OR NEW.retired=1)
BEGIN
UPDATE matrix_permit SET held=0,released_at_ms=coalesce(NEW.terminal_at_ms,NEW.retired_at_ms)
WHERE permit_id=NEW.permit_id AND held=1;
END;
UPDATE matrix_permit SET held=0,released_at_ms=(
  SELECT coalesce(terminal_at_ms,retired_at_ms) FROM attempt WHERE attempt.attempt_id=matrix_permit.attempt_id
) WHERE held=1 AND attempt_id IN (SELECT attempt_id FROM attempt WHERE permit_id IS NOT NULL AND (state='terminal' OR retired=1));"#.into(),
            r#"CREATE TABLE ci_driver_transition (
 run_id TEXT NOT NULL REFERENCES run(run_id), authorization INTEGER NOT NULL CHECK(authorization>=0),
 revision INTEGER NOT NULL CHECK(revision>0), record TEXT NOT NULL,
 PRIMARY KEY(run_id,authorization,revision)
) WITHOUT ROWID;"#.into(),
            r#"CREATE TABLE workflow_artifact (
 run_id TEXT NOT NULL REFERENCES run(run_id),
 workflow TEXT NOT NULL,
 name TEXT NOT NULL,
 version INTEGER NOT NULL CHECK(version>0),
 producer TEXT NOT NULL REFERENCES attempt(attempt_id),
 action_key TEXT NOT NULL,
 fingerprint TEXT NOT NULL,
 receipt_json TEXT NOT NULL,
 PRIMARY KEY(run_id,workflow,name,version),
 UNIQUE(producer,action_key)
) WITHOUT ROWID;"#.into(),
            "CREATE TABLE ci_test_timing(scope TEXT NOT NULL, worker TEXT NOT NULL, identity TEXT NOT NULL, millis INTEGER NOT NULL CHECK(millis>=0), PRIMARY KEY(scope,identity,worker)) WITHOUT ROWID;".into(),
            r#"CREATE TABLE workflow_download (
 consumer TEXT NOT NULL REFERENCES attempt(attempt_id),
 action_key TEXT NOT NULL,
 fingerprint TEXT NOT NULL,
 selection_json TEXT NOT NULL,
 PRIMARY KEY(consumer,action_key)
) WITHOUT ROWID;"#.into(),
            r#"CREATE TABLE workflow_upload_inflight (
    producer TEXT NOT NULL REFERENCES attempt(attempt_id),
    action_key TEXT NOT NULL,
    workflow TEXT NOT NULL,
    name TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    PRIMARY KEY(producer,action_key)
) WITHOUT ROWID;"#.into(),
            // Step 13 released only the held permits of an ended attempt. A
            // permit that an earlier binary reserved and never bound to an
            // attempt belongs to nothing that can end: a reservation and its
            // binding are one commit now, so no such permit is in use while
            // this runs, and its slot is given back.
            "UPDATE matrix_permit SET held=0,released_at_ms=coalesce(released_at_ms,created_at_ms) WHERE held=1 AND attempt_id IS NULL;".into(),
            // Spec chapter 24. The notice a retired worker identity was sent
            // (the identity tombstone itself rejects updates, so it cannot
            // carry this), and the members removed from the list whose
            // machine may still be reachable over SSH.
            r#"CREATE TABLE identity_retired_notice (
    worker_id  TEXT NOT NULL,
    boot_id    TEXT NOT NULL,
    sent_at_ms INTEGER NOT NULL,
    PRIMARY KEY (worker_id, boot_id)
) WITHOUT ROWID;
CREATE TABLE removed_member (
    name          TEXT PRIMARY KEY,
    removed_at_ms INTEGER NOT NULL
) WITHOUT ROWID;"#.into(),
        ],
        // Persisted payloads now carry execution constraints. A previous
        // binary must refuse this version instead of dropping unknown fields.
        DbKind::Worker => vec![format!("{COMMON}{WORKER_V1}"), "INSERT INTO schema_meta(key,value) VALUES('execution_requirements_v1','1');".into(), "INSERT INTO schema_meta(key,value) VALUES('frozen_toolchains_v1','1');".into(), WORKER_V4.to_string()],
    }
}

/// The compatibility table for a list of migration steps.
pub(crate) fn compat(step_count: usize) -> CompatTable {
    CompatTable {
        current: step_count as u32,
        min_readable: 1,
        min_migratable: 1,
    }
}
