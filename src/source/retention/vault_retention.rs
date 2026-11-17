//! Explicit synchronous parent Vault deletion; no resident caller.
use super::*;
use crate::ids::RunId;
use crate::secrets::{key::ClusterKey, vault::Vault};
use serde::{Deserialize, Serialize};

/// Pinned before parent deletion is ever possible. An absent/replaced index
/// after registration is an availability failure, never an unregistered worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VaultRegistration {
    source: PathBuf,
    root: (u64, u64),
    database: (u64, u64),
}
#[derive(Debug, Default)]
pub struct VaultCursor {
    run: Option<String>,
}
#[derive(Debug, Default)]
pub struct VaultPage {
    pub busy: bool,
    pub held: bool,
    pub examined: u32,
    pub claimed: u32,
    pub paths: u32,
    pub removed: u32,
    pub unknown: u32,
    /// Orphaned publication temporaries unlinked (separate bound of 32).
    pub tmp_removed: u32,
    /// Entries named like a publication temporary that the sweep left in
    /// place: not provably ours, or not removable. Leftovers, not retention
    /// failures, so they are not part of `unknown`.
    pub tmp_retained: u32,
    pub errors: Vec<String>,
}
// st_dev is signed on macOS and u64 on Linux.
#[allow(clippy::unnecessary_cast)]
impl VaultRegistration {
    pub(crate) fn matches_parent(&self, state: &Path) -> bool {
        self.source == state.join("sources")
    }
    fn root_file(&self) -> Result<File> {
        ensure!(
            self.source.is_absolute(),
            "relative Vault source registration"
        );
        let mut file: File = rustix::fs::open("/", DIR, rustix::fs::Mode::empty())?.into();
        for part in self.source.components() {
            match part {
                std::path::Component::RootDir => {}
                std::path::Component::Normal(name) => {
                    file = rustix::fs::openat(&file, name, DIR, rustix::fs::Mode::empty())?.into()
                }
                _ => anyhow::bail!("noncanonical Vault source registration"),
            }
        }
        let m = file.metadata()?;
        ensure!(
            m.is_dir()
                && m.uid() == rustix::process::geteuid().as_raw()
                && m.mode() & 0o077 == 0
                && (m.dev(), m.ino()) == self.root,
            "Vault source root changed"
        );
        Ok(file)
    }
    fn check(&self, c: &Connection) -> Result<()> {
        let file = self.root_file()?;
        let before =
            rustix::fs::statat(&file, "retention.db", rustix::fs::AtFlags::SYMLINK_NOFOLLOW)?;
        ensure!(
            rustix::fs::FileType::from_raw_mode(before.st_mode)
                == rustix::fs::FileType::RegularFile
                && before.st_uid == rustix::process::geteuid().as_raw()
                && before.st_nlink == 1
                && before.st_mode & 0o077 == 0
                && (before.st_dev as u64, before.st_ino) == self.database,
            "Vault source index changed"
        );
        let mut moved: std::ffi::c_int = -1;
        // SAFETY: synchronous connection remains owned/live; main is static,
        // moved is a valid aligned writable c_int. No validation DB FD is opened.
        let rc = unsafe {
            rusqlite::ffi::sqlite3_file_control(
                c.handle(),
                c"main".as_ptr(),
                rusqlite::ffi::SQLITE_FCNTL_HAS_MOVED,
                std::ptr::from_mut(&mut moved).cast(),
            )
        };
        let after =
            rustix::fs::statat(&file, "retention.db", rustix::fs::AtFlags::SYMLINK_NOFOLLOW)?;
        ensure!(
            rc == rusqlite::ffi::SQLITE_OK
                && moved == 0
                && (before.st_dev, before.st_ino) == (after.st_dev, after.st_ino),
            "Vault live source index moved or unknown"
        );
        self.root_file()?;
        Ok(())
    }
}
fn registered_connection(registration: &VaultRegistration) -> Result<Connection> {
    registration.root_file()?;
    let c = Connection::open_with_flags(
        registration.source.join("retention.db"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    c.busy_timeout(std::time::Duration::from_secs(1))?;
    registration.check(&c)?;
    let raw: Option<String> = c
        .query_row(
            "SELECT value FROM meta WHERE key='vault_registration'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(raw) = raw {
        ensure!(
            serde_json::from_str::<VaultRegistration>(&raw)? == *registration,
            "Vault index and classification pin disagree"
        );
    }
    Ok(c)
}
/// Recover parent classification without trusting the optional Vault marker.
/// Never creates an index, and never opens a separate validation database FD.
#[allow(clippy::unnecessary_cast)]
pub(crate) fn recover_vault_registration(state: &Path) -> Result<Option<VaultRegistration>> {
    let source = state.join("sources");
    let mut root: File = rustix::fs::open("/", DIR, rustix::fs::Mode::empty())?.into();
    for part in source.components() {
        match part {
            std::path::Component::RootDir => {}
            std::path::Component::Normal(name) => {
                root = match rustix::fs::openat(&root, name, DIR, rustix::fs::Mode::empty()) {
                    Ok(fd) => fd.into(),
                    Err(rustix::io::Errno::NOENT) if name == "sources" => return Ok(None),
                    Err(e) => return Err(e.into()),
                };
            }
            _ => anyhow::bail!("noncanonical Vault source path"),
        }
    }
    let metadata = root.metadata()?;
    let db = rustix::fs::statat(&root, "retention.db", rustix::fs::AtFlags::SYMLINK_NOFOLLOW)?;
    let observed = VaultRegistration {
        source,
        root: (metadata.dev(), metadata.ino()),
        database: (db.st_dev as u64, db.st_ino),
    };
    let c = registered_connection(&observed)?;
    let raw: Option<String> = c
        .query_row(
            "SELECT value FROM meta WHERE key='vault_registration'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(raw) = raw {
        let pinned: VaultRegistration = serde_json::from_str(&raw)?;
        ensure!(pinned == observed, "recovered Vault registration changed");
        pinned.check(&c)?;
        return Ok(Some(pinned));
    }
    let claims: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM vault_delete_claim)", [], |r| {
        r.get(0)
    })?;
    ensure!(
        !claims,
        "parent Vault registration missing from claimed index"
    );
    observed.check(&c)?;
    Ok(None)
}
pub(crate) fn check_vault_registration(registration: &VaultRegistration) -> Result<()> {
    let c = registered_connection(registration)?;
    c.prepare("SELECT run_id FROM vault_delete_claim LIMIT 0")?;
    registration.check(&c)
}
/// Caller holds Vault SH. This deliberately acquires no source lock and no actor.
pub(crate) fn check_vault_run(registration: &VaultRegistration, run: &RunId) -> Result<()> {
    let c = registered_connection(registration)?;
    let expired: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM vault_delete_claim WHERE run_id=?1)",
        [run.as_str()],
        |r| r.get(0),
    )?;
    registration.check(&c)?;
    ensure!(!expired, "parent Vault run expired");
    Ok(())
}
impl Index {
    #[allow(clippy::unnecessary_cast)]
    fn vault_registration(&self, c: &Connection) -> Result<VaultRegistration> {
        self.check_anchor()?;
        let root = self.root.metadata()?;
        let db = rustix::fs::statat(
            &*self.root,
            "retention.db",
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?;
        let registration = VaultRegistration {
            source: self.database.parent().context("index parent")?.into(),
            root: (root.dev(), root.ino()),
            database: (db.st_dev as u64, db.st_ino),
        };
        registration.check(c)?;
        Ok(registration)
    }
}
impl SourceStore {
    /// Holds source EX → Vault EX throughout proof, durable claim, unlink,
    /// parent fsync and completion. A busy reader or clock Hold consumes no cursor.
    /// No source expiry is created; Vault has its own zero-delay persisted clock.
    #[allow(clippy::too_many_arguments)]
    pub fn vault_retention_page(
        &self,
        owner: &crate::storage::CoordinatorStore,
        vault: &Vault,
        key: &ClusterKey,
        cursor: &mut VaultCursor,
        wall: u64,
        mono: u64,
        boot: Option<String>,
    ) -> Result<VaultPage> {
        let busy = |e: &anyhow::Error| {
            e.chain().any(|cause| {
                cause
                    .downcast_ref::<io::Error>()
                    .is_some_and(|e| e.kind() == io::ErrorKind::WouldBlock)
            })
        };
        let _source = match self.retention.guard(true) {
            Ok(g) => g,
            Err(e) if busy(&e) => {
                return Ok(VaultPage {
                    busy: true,
                    ..Default::default()
                })
            }
            Err(e) => return Err(e),
        };
        ensure!(
            vault.matches_source(&self.retention.path),
            "Vault is not this parent's Vault"
        );
        let lease = match vault.lease(true) {
            Ok(g) => g,
            Err(e) => {
                let e = anyhow::Error::new(e);
                if busy(&e) {
                    return Ok(VaultPage {
                        busy: true,
                        ..Default::default()
                    });
                }
                return Err(e);
            }
        };
        #[cfg(test)]
        self.retention.pause_page();
        self.retention.check_anchor()?;
        let identity = owner.check_input_store(&self.retention.path, &self.retention.root)?;
        let c = self.retention.connect()?;
        let registration = self.retention.vault_registration(&c)?;
        let pinned: Option<String> = c
            .query_row(
                "SELECT value FROM meta WHERE key='retention_owner'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        ensure!(
            pinned.as_deref().is_none_or(|old| old == identity),
            "Vault retention owner changed"
        );
        c.execute(
            "INSERT OR IGNORE INTO meta VALUES('retention_owner',?1)",
            [&identity],
        )?;
        let raw = serde_json::to_string(&registration)?;
        let old: Option<String> = c
            .query_row(
                "SELECT value FROM meta WHERE key='vault_registration'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        ensure!(
            old.as_ref().is_none_or(|old| old == &raw),
            "Vault registration changed"
        );
        // Persist independent classification before marker publication or claims.
        c.execute(
            "INSERT OR IGNORE INTO meta VALUES('vault_registration',?1)",
            [&raw],
        )?;
        lease.register(&registration)?;
        // A crashed publication is not retained data: no run owns the
        // temporary name, and the exclusive lease proves no writer is alive.
        // So this runs before, and regardless of, the clock and legacy Holds.
        let swept = lease.sweep_orphan_tmp(32)?;
        let swept = VaultPage {
            tmp_removed: swept.removed,
            tmp_retained: swept.retained,
            ..Default::default()
        };
        let previous: Option<String> = c
            .query_row("SELECT value FROM meta WHERE key='vault_clock'", [], |r| {
                r.get(0)
            })
            .optional()?;
        let previous = previous
            .as_deref()
            .map(serde_json::from_str::<crate::results::clock_policy::ClockState>)
            .transpose()?;
        let step =
            crate::results::clock_policy::step(previous.as_ref(), boot.as_deref(), wall, mono, 0);
        if let Some(state) = step.state {
            c.execute("INSERT INTO meta VALUES('vault_clock',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[serde_json::to_string(&state)?])?;
        }
        let Some(now) = step.cutoff else {
            return Ok(VaultPage {
                held: true,
                ..swept
            });
        };
        let mut report = swept;
        let mut budget = 64 * 1024 * 1024u64;
        let pending: Vec<(String, String)> = {
            let mut s=c.prepare("SELECT run_id,claim_json FROM vault_delete_claim WHERE done=0 ORDER BY run_id LIMIT 32")?;
            let rows = s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        if !pending.is_empty() {
            for (run, raw) in pending {
                report.paths += 1;
                let run = RunId::parse(&run)?;
                let claim: crate::secrets::vault::Claim = serde_json::from_str(&raw)?;
                let result = (|| {
                    lease.reserve(&run, &mut budget, 1)?;
                    registration.check(&c)?;
                    ensure!(
                        owner.check_input_store(&self.retention.path, &self.retention.root)?
                            == identity,
                        "Vault owner changed before replay"
                    );
                    lease.drain(&run, &claim)?;
                    c.execute(
                        "UPDATE vault_delete_claim SET done=1 WHERE run_id=?1 AND done=0",
                        [run.as_str()],
                    )?;
                    Ok(())
                })();
                record(&mut report, result);
            }
            return Ok(report);
        }
        let unknown:bool=c.query_row("SELECT (SELECT value FROM meta WHERE key='legacy_registration')<>'done' OR EXISTS(SELECT 1 FROM root WHERE source IS NULL) OR EXISTS(SELECT 1 FROM graph WHERE done=0)",[],|r|r.get(0))?;
        if unknown {
            return Ok(VaultPage {
                held: true,
                ..report
            });
        }
        let candidates: Vec<(String, String)> = {
            let mut s=c.prepare("SELECT id,source FROM root WHERE kind='run' AND id>coalesce(?1,'') ORDER BY id LIMIT 32")?;
            let rows = s.query_map([&cursor.run], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        if candidates.is_empty() {
            cursor.run = None;
            return Ok(report);
        }
        for (run, source) in candidates {
            report.examined += 1;
            cursor.run = Some(run.clone());
            let result = (|| {
                let done: bool = c.query_row(
                    "SELECT EXISTS(SELECT 1 FROM vault_delete_claim WHERE run_id=?1)",
                    [&run],
                    |r| r.get(0),
                )?;
                if done {
                    return Ok(());
                }
                let parsed = RunId::parse(&run)?;
                let source_id = super::super::classify::source_id_of_ci(&source)
                    .context("invalid Vault source")?;
                let validated = self.retention.load_validated(&source, budget)?;
                budget = budget.saturating_sub(
                    validated
                        .metadata
                        .as_ref()
                        .context("unmeasured Vault source manifest")?
                        .len(),
                );
                validated.check(&self.retention)?;
                // No required secret bytes can be lost for a secret-free run;
                // fencing its empty load would wrongly disable source-only retry.
                if validated.manifest.secret_files.is_empty() {
                    ensure!(
                        !lease.has(&parsed)?,
                        "unexpected Vault bundle for a secret-free run"
                    );
                    return Ok(());
                }
                let Some((ended, binding)) = owner.input_retention_binding(
                    &parsed,
                    &source_id,
                    &validated.manifest.manifest_digest,
                )?
                else {
                    return Ok(());
                };
                if ended > now {
                    return Ok(());
                }
                ensure!(budget >= 65536, "Vault run binding page budget exhausted");
                budget -= 65536;
                let binding = self.retention.exact_run_binding(&run, &source, &binding)?;
                lease.reserve(&parsed, &mut budget, 2)?;
                let claim = vault.claim_for_retention(
                    &lease,
                    &parsed,
                    &validated.manifest.secret_files,
                    key,
                )?;
                ensure!(
                    owner.check_input_store(&self.retention.path, &self.retention.root)?
                        == identity,
                    "Vault owner changed before claim"
                );
                registration.check(&c)?;
                c.execute("INSERT INTO vault_delete_claim(run_id,source_id,binding_digest,claim_json,done) VALUES(?1,?2,?3,?4,0)",params![run,source,binding,serde_json::to_string(&claim)?])?;
                report.claimed += 1;
                report.paths += 1;
                crate::secrets::vault::retention_fault("after_claim")?;
                registration.check(&c)?;
                ensure!(
                    owner.check_input_store(&self.retention.path, &self.retention.root)?
                        == identity,
                    "Vault owner changed after claim"
                );
                lease.drain(&parsed, &claim)?;
                c.execute(
                    "UPDATE vault_delete_claim SET done=1 WHERE run_id=?1 AND done=0",
                    [&run],
                )?;
                report.removed += 1;
                Ok(())
            })();
            if let Err(error) = result {
                report.unknown += 1;
                report.errors.push(format!("{error:#}"));
            }
        }
        Ok(report)
    }
}
fn record(report: &mut VaultPage, result: Result<()>) {
    match result {
        Ok(()) => report.removed += 1,
        Err(error) => {
            report.unknown += 1;
            report.errors.push(format!("{error:#}"));
        }
    }
}
