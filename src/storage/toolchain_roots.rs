//! Spec 25.7 (b): the rows of the toolchain roots (`toolchains/<identity>/`)
//! and of who uses them. Every function is one transaction of the worker DB,
//! so a bind and a claim of a removal are in a single order: a bind that
//! comes first leaves a reference the claim sees, and a claim that comes
//! first leaves `evicting`, which a bind treats as no root.

use rusqlite::{params, OptionalExtension};

use super::Result;
use super::WorkerStore;

/// The marker that the roots found on disk at the first start with these
/// tables have their rows (written in the same transaction as the rows).
const SEEDED: &str = "toolchain_roots_seeded";
/// Why the passes have removed nothing, and since when (`<reason> <ms>`),
/// for `doctor`. Not there while a pass can remove.
const HELD: &str = "toolchain_roots_held";
/// How many removals failed in a way the next pass does not cure by waiting
/// (the root went back to `live`), for `doctor`.
const EVICT_FAILURES: &str = "toolchain_evict_failures";

/// What `doctor` says of the passes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RootsStatus {
    /// The reason the passes stop, and since when.
    pub held: Option<(String, u64)>,
    pub evict_failures: u64,
}

/// What a bind found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bind {
    /// The root is `live` and is now referenced: it may be read and run.
    Bound,
    /// No row, or `evicting`: the root is treated as not there.
    Absent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootRow {
    pub identity: String,
    pub evicting: bool,
    pub last_used_ms: u64,
    pub bytes: Option<u64>,
    /// When the bytes were last measured or tried (a failure stamps it too,
    /// so one unmeasurable root does not keep the others unmeasured).
    pub measured_ms: Option<u64>,
}

impl WorkerStore {
    /// Whether the roots that were on disk before these tables have rows.
    pub fn toolchain_roots_seeded(&self) -> Result<bool> {
        self.db.call(|c| {
            Ok(
                c.query_row("SELECT 1 FROM schema_meta WHERE key = ?1", [SEEDED], |_| {
                    Ok(())
                })
                .optional()?
                .is_some(),
            )
        })
    }

    /// Give each root in `identities` (each with a receipt) a `live` row, once:
    /// `false` when it was done before. The rows and the marker are one
    /// transaction, so a database put back by a rollback does it again.
    pub fn seed_toolchain_roots(&self, identities: Vec<String>, now_ms: u64) -> Result<bool> {
        self.db.write(move |tx| {
            let done = tx
                .query_row(
                    "SELECT 1 FROM schema_meta WHERE key = ?1",
                    [SEEDED],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if done {
                return Ok(false);
            }
            for identity in &identities {
                tx.execute(
                    "INSERT OR IGNORE INTO toolchain_root (identity, state, last_used_ms) VALUES (?1, 'live', ?2)",
                    params![identity, now_ms as i64],
                )?;
            }
            // The value is the time of the first start: an attempt accepted
            // before it bound its roots without a reference.
            tx.execute(
                "INSERT INTO schema_meta (key, value) VALUES (?1, ?2)",
                params![SEEDED, now_ms.to_string()],
            )?;
            Ok(true)
        })
    }

    /// Whether an attempt accepted before the first start with these tables
    /// is still not terminal: it may run from a root without a reference.
    pub fn toolchain_attempt_from_before(&self) -> Result<bool> {
        self.db.call(|c| {
            Ok(c.query_row(
                "SELECT 1 FROM worker_attempt WHERE state <> 'terminal'
                   AND accepted_at_ms <= (SELECT CAST(value AS INTEGER) FROM schema_meta WHERE key = ?1)
                 LIMIT 1",
                [SEEDED],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
        })
    }

    /// Bind `consumer` to the root before anything of it is read. The
    /// consumer is an attempt when the worker has a row for it (its member
    /// is the row's), and otherwise a warm-up. An attempt that is terminal
    /// already gets no reference and is answered `Absent`.
    pub fn toolchain_bind(&self, identity: &str, consumer: &str, now_ms: u64) -> Result<Bind> {
        let (identity, consumer) = (identity.to_string(), consumer.to_string());
        self.db.write(move |tx| {
            let live = tx
                .query_row(
                    "SELECT state = 'live' FROM toolchain_root WHERE identity = ?1",
                    [&identity],
                    |r| r.get::<_, bool>(0),
                )
                .optional()?
                .unwrap_or(false);
            if !live {
                return Ok(Bind::Absent);
            }
            let attempt: Option<(String, String)> = tx
                .query_row(
                    "SELECT coordinator_id, state FROM worker_attempt WHERE attempt_id = ?1",
                    [&consumer],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            match attempt {
                Some((_, state)) if state == "terminal" => return Ok(Bind::Absent),
                Some((member, _)) => tx.execute(
                    "INSERT OR IGNORE INTO toolchain_ref (identity, kind, holder, coordinator_id) VALUES (?1, 'attempt', ?2, ?3)",
                    params![identity, consumer, member],
                )?,
                None => tx.execute(
                    "INSERT OR IGNORE INTO toolchain_ref (identity, kind, holder) VALUES (?1, 'warmup', ?2)",
                    params![identity, consumer],
                )?,
            };
            tx.execute(
                "UPDATE toolchain_root SET last_used_ms = max(last_used_ms, ?2) WHERE identity = ?1",
                params![identity, now_ms as i64],
            )?;
            Ok(Bind::Bound)
        })
    }

    /// A builder that installs into the root references it first. `false`
    /// (and no reference) while the root is being removed: the builder waits
    /// for the removal to finish.
    pub fn toolchain_hold_for_install(&self, identity: &str, builder: &str) -> Result<bool> {
        let (identity, builder) = (identity.to_string(), builder.to_string());
        self.db.write(move |tx| {
            let evicting = tx
                .query_row(
                    "SELECT 1 FROM toolchain_root WHERE identity = ?1 AND state = 'evicting'",
                    [&identity],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if evicting {
                return Ok(false);
            }
            tx.execute(
                "INSERT OR IGNORE INTO toolchain_ref (identity, kind, holder) VALUES (?1, 'builder', ?2)",
                params![identity, builder],
            )?;
            Ok(true)
        })
    }

    /// The install finished with a receipt: the root is `live`.
    pub fn toolchain_installed(&self, identity: &str, now_ms: u64) -> Result<()> {
        let identity = identity.to_string();
        self.db.write(move |tx| {
            tx.execute(
                "INSERT INTO toolchain_root (identity, state, last_used_ms) VALUES (?1, 'live', ?2)
                 ON CONFLICT (identity) DO UPDATE SET last_used_ms = max(last_used_ms, ?2), bytes = NULL",
                params![identity, now_ms as i64],
            )?;
            Ok(())
        })
    }

    /// References of a builder or a template to the roots it was made with,
    /// in one transaction with the check that each is `live`: `false` (and
    /// no reference) when one has no row or is being removed.
    pub fn toolchain_add_refs(
        &self,
        identities: Vec<String>,
        kind: &'static str,
        holder: &str,
    ) -> Result<bool> {
        let holder = holder.to_string();
        self.db.write(move |tx| {
            for identity in &identities {
                let live = tx
                    .query_row(
                        "SELECT state = 'live' FROM toolchain_root WHERE identity = ?1",
                        [identity],
                        |r| r.get::<_, bool>(0),
                    )
                    .optional()?
                    .unwrap_or(false);
                if !live {
                    return Ok(false);
                }
            }
            for identity in &identities {
                tx.execute(
                    "INSERT OR IGNORE INTO toolchain_ref (identity, kind, holder) VALUES (?1, ?2, ?3)",
                    params![identity, kind, holder],
                )?;
            }
            Ok(true)
        })
    }

    /// Let go of every reference of one holder.
    pub fn toolchain_release(&self, kind: &'static str, holder: &str) -> Result<()> {
        let holder = holder.to_string();
        self.db.write(move |tx| {
            tx.execute(
                "DELETE FROM toolchain_ref WHERE kind = ?1 AND holder = ?2",
                params![kind, holder],
            )?;
            Ok(())
        })
    }

    /// Let go of every reference of a kind (the warm-ups of an earlier start
    /// of the agent).
    pub fn toolchain_release_kind(&self, kind: &'static str) -> Result<()> {
        self.db.write(move |tx| {
            tx.execute("DELETE FROM toolchain_ref WHERE kind = ?1", [kind])?;
            Ok(())
        })
    }

    /// The holders of a kind that have references.
    pub fn toolchain_holders(&self, kind: &'static str) -> Result<Vec<String>> {
        self.db.call(move |c| {
            let mut stmt =
                c.prepare("SELECT DISTINCT holder FROM toolchain_ref WHERE kind = ?1")?;
            let rows = stmt.query_map([kind], |r| r.get(0))?;
            Ok(rows.collect::<rusqlite::Result<Vec<String>>>()?)
        })
    }

    pub fn toolchain_roots(&self) -> Result<Vec<RootRow>> {
        self.db.call(|c| {
            let mut stmt = c.prepare(
                "SELECT identity, state = 'evicting', last_used_ms, bytes, measured_ms FROM toolchain_root ORDER BY last_used_ms, identity",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok(RootRow {
                    identity: r.get(0)?,
                    evicting: r.get(1)?,
                    last_used_ms: r.get::<_, i64>(2)? as u64,
                    bytes: r.get::<_, Option<i64>>(3)?.map(|b| b as u64),
                    measured_ms: r.get::<_, Option<i64>>(4)?.map(|b| b as u64),
                })
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Whether anything references the root.
    pub fn toolchain_referenced(&self, identity: &str) -> Result<bool> {
        let identity = identity.to_string();
        self.db.call(move |c| {
            Ok(c.query_row(
                "SELECT 1 FROM toolchain_ref WHERE identity = ?1 LIMIT 1",
                [&identity],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
        })
    }

    /// The claim of a removal: the root becomes `evicting` only if it is
    /// `live`, nothing references it and (with `unused_before`) it was last
    /// used before that time. One transaction with the check.
    pub fn toolchain_claim(&self, identity: &str, unused_before: Option<u64>) -> Result<bool> {
        let identity = identity.to_string();
        self.db.write(move |tx| {
            Ok(tx.execute(
                "UPDATE toolchain_root SET state = 'evicting'
                 WHERE identity = ?1 AND state = 'live'
                   AND NOT EXISTS (SELECT 1 FROM toolchain_ref WHERE identity = ?1)
                   AND (?2 IS NULL OR last_used_ms < ?2)",
                params![identity, unused_before.map(|t| t as i64)],
            )? == 1)
        })
    }

    /// The root of a claimed removal is gone: its row goes.
    pub fn toolchain_forget(&self, identity: &str) -> Result<()> {
        let identity = identity.to_string();
        self.db.write(move |tx| {
            tx.execute(
                "DELETE FROM toolchain_root WHERE identity = ?1 AND state = 'evicting'",
                [&identity],
            )?;
            Ok(())
        })
    }

    /// The bytes of a root, measured at `now_ms`; `None` when measuring
    /// failed (the time is still stamped).
    pub fn toolchain_set_bytes(
        &self,
        identity: &str,
        bytes: Option<u64>,
        now_ms: u64,
    ) -> Result<()> {
        let identity = identity.to_string();
        self.db.write(move |tx| {
            tx.execute(
                "UPDATE toolchain_root SET bytes = ?2, measured_ms = ?3 WHERE identity = ?1",
                params![identity, bytes.map(|b| b as i64), now_ms as i64],
            )?;
            Ok(())
        })
    }

    /// A claimed removal that failed in a way waiting does not cure: the root
    /// is `live` again, counted as used now (so it is not picked again at
    /// once), and the failure is counted.
    pub fn toolchain_unclaim(&self, identity: &str, now_ms: u64) -> Result<()> {
        let identity = identity.to_string();
        self.db.write(move |tx| {
            let n = tx.execute(
                "UPDATE toolchain_root SET state = 'live', last_used_ms = max(last_used_ms, ?2)
                 WHERE identity = ?1 AND state = 'evicting'",
                params![identity, now_ms as i64],
            )?;
            if n == 1 {
                tx.execute(
                    "INSERT INTO schema_meta (key, value) VALUES (?1, '1')
                     ON CONFLICT (key) DO UPDATE SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)",
                    [EVICT_FAILURES],
                )?;
            }
            Ok(())
        })
    }

    /// Record why the pass stopped (`None`: it did not). The time is kept
    /// while the reason stays the same.
    pub fn toolchain_set_held(&self, reason: Option<&'static str>, now_ms: u64) -> Result<()> {
        self.db.write(move |tx| {
            let old: Option<String> = tx
                .query_row("SELECT value FROM schema_meta WHERE key = ?1", [HELD], |r| {
                    r.get(0)
                })
                .optional()?;
            match reason {
                None if old.is_some() => {
                    tx.execute("DELETE FROM schema_meta WHERE key = ?1", [HELD])?;
                }
                None => {}
                Some(reason) => {
                    let same = old
                        .as_deref()
                        .and_then(|v| v.split_once(' '))
                        .is_some_and(|(r, _)| r == reason);
                    if !same {
                        tx.execute(
                            "INSERT INTO schema_meta (key, value) VALUES (?1, ?2)
                             ON CONFLICT (key) DO UPDATE SET value = ?2",
                            params![HELD, format!("{reason} {now_ms}")],
                        )?;
                    }
                }
            }
            Ok(())
        })
    }

    /// What the passes recorded, for `doctor`.
    pub fn toolchain_status(&self) -> Result<RootsStatus> {
        self.db.call(|c| {
            let get = |key: &str| -> rusqlite::Result<Option<String>> {
                c.query_row("SELECT value FROM schema_meta WHERE key = ?1", [key], |r| {
                    r.get(0)
                })
                .optional()
            };
            let held = get(HELD)?.and_then(|v| {
                let (reason, since) = v.split_once(' ')?;
                Some((reason.to_string(), since.parse().ok()?))
            });
            let evict_failures = get(EVICT_FAILURES)?
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            Ok(RootsStatus {
                held,
                evict_failures,
            })
        })
    }
}

#[cfg(test)]
mod tests;
