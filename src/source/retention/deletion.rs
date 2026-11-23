//! Parent-only source retention, invoked by the resident bounded driver.
//! Source EX spans the synchronous operation, including unlink/fsync/SQLite drain.
use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::os::unix::fs::MetadataExt;

mod abandon;
mod import_release;
mod prune;
mod scratch;
pub use abandon::{AbandonCursor, AbandonReport, AbandonScope};
pub use import_release::{abandoned_imports_read_only, ImportReleaseReport};
#[cfg(test)]
pub use prune::set_prune_fault;
pub use prune::{ExclusiveGuard, PruneBudget, PruneInputs};
pub(in crate::source) use scratch::Reserved;
#[cfg(test)]
pub(super) use scratch::STAGING;

/// The gate that holds every deletion of an object of the source store
/// (spec 25.5, step 4): nothing is unlinked while an unindexed manifest, a
/// root of unknown membership or an unfinished graph could need it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Gate {
    /// The registration of the manifests from before the index is not done.
    LegacyRegistration,
    /// A capture is in progress (a root of unknown membership).
    Capture,
    /// A graph of a root is unfinished.
    Graph,
}

impl Gate {
    pub fn as_str(self) -> &'static str {
        match self {
            Gate::LegacyRegistration => "legacy_registration",
            Gate::Capture => "capture",
            Gate::Graph => "graph",
        }
    }

    /// The first gate that is closed in the index `c`, if any.
    pub fn closed(c: &Connection) -> Result<Option<Gate>> {
        let (legacy, capture, graph): (bool, bool, bool) = c.query_row(
            "SELECT coalesce((SELECT value FROM meta WHERE key='legacy_registration'),'')<>'done', EXISTS(SELECT 1 FROM root WHERE source IS NULL AND kind NOT IN ('abandoned','receipt')), EXISTS(SELECT 1 FROM graph WHERE done=0)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        Ok(if legacy {
            Some(Gate::LegacyRegistration)
        } else if capture {
            Some(Gate::Capture)
        } else if graph {
            Some(Gate::Graph)
        } else {
            None
        })
    }

    /// [`Self::closed`] for the source index below `state_dir`, opened read
    /// only (`doctor`). `None` when there is no index.
    pub fn closed_read_only(state_dir: &std::path::Path) -> Result<Option<Gate>> {
        let path = state_dir.join("sources/retention.db");
        if !path.is_file() {
            return Ok(None);
        }
        // As `inspect_owners`: read only, no symlink, a short wait.
        let c = Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        c.busy_timeout(std::time::Duration::from_secs(1))?;
        Self::closed(&c)
    }
}

type OpenedParent = (File, String, Vec<(u64, u64)>);
const PAGE: usize = 32;
const BYTE_BUDGET: u64 = 64 * 1024 * 1024;

#[derive(Default)]
pub struct RetentionCursor {
    run: Option<String>,
    object: String,
    validated: Option<Validated>,
}
#[derive(Debug, Default)]
pub struct RetentionPage {
    pub busy: bool,
    pub held: bool,
    pub examined: u32,
    pub claimed: u32,
    /// Distinct claimed file paths processed (new or replay), at most 32.
    pub paths: u32,
    pub removed: u32,
    pub shared: u32,
    pub unknown: u32,
    pub run: Option<String>,
    pub errors: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Identity {
    dev: u64,
    ino: u64,
    size: u64,
    uid: u32,
    mode: u32,
    links: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}
impl Identity {
    fn of(file: &File) -> Result<Self> {
        let m = file.metadata()?;
        Ok(Self {
            dev: m.dev(),
            ino: m.ino(),
            size: m.len(),
            uid: m.uid(),
            mode: m.mode(),
            links: m.nlink(),
            modified: (m.mtime(), m.mtime_nsec()),
            changed: (m.ctime(), m.ctime_nsec()),
        })
    }
    fn leaf(&self) -> bool {
        self.mode & 0o170000 == 0o100000
            && self.uid == rustix::process::geteuid().as_raw()
            && self.links == 1
            && self.mode & 0o022 == 0
            && self.mode & 0o111 == 0
            && self.mode & 0o7000 == 0
    }
}
#[derive(Debug, Serialize, Deserialize)]
struct Claim {
    object: String,
    path: String,
    incarnation: i64,
    store: (u64, u64),
    parents: Vec<(u64, u64)>,
    leaf: Identity,
    digest: String,
    /// The claim of a file that only an abandoned capture held (spec 25.4
    /// (b)). Its deletion follows an event, not a period, so the pass over
    /// the abandoned captures replays it without waiting for a clock; every
    /// other claim is replayed by the page of expired runs.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    abandoned: bool,
}
fn directory(file: &File, private: bool) -> Result<(u64, u64)> {
    let m = file.metadata()?;
    ensure!(
        m.is_dir()
            && m.uid() == rustix::process::geteuid().as_raw()
            && m.mode() & if private { 0o077 } else { 0o022 } == 0,
        "unsafe source deletion directory"
    );
    Ok((m.dev(), m.ino()))
}
fn components(object: &str, path: &str) -> Result<Vec<String>> {
    let expected = if let Some(id) = object.strip_prefix("m:") {
        ensure!(
            super::super::classify::source_id_of_ci(id).is_some(),
            "invalid owned source id"
        );
        format!("manifests/{id}.json")
    } else if let Some(hash) = object.strip_prefix("b:") {
        ensure!(
            super::super::diff::is_blob_hash(hash),
            "invalid source digest"
        );
        format!("objects/{}/{hash}", &hash[..2])
    } else {
        anyhow::bail!("unknown source object class")
    };
    ensure!(path == expected, "source object path mismatch");
    Ok(path.split('/').map(str::to_owned).collect())
}
impl Index {
    pub(super) fn exact_run_binding(
        &self,
        run: &str,
        source: &str,
        binding: &str,
    ) -> Result<String> {
        let runs: File =
            rustix::fs::openat(&*self.root, "runs", DIR, rustix::fs::Mode::empty())?.into();
        directory(&runs, false)?;
        let record: File = rustix::fs::openat(
            &runs,
            run,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?
        .into();
        let identity = Identity::of(&record)?;
        ensure!(
            identity.leaf() && identity.size <= 65536,
            "unsafe source run binding"
        );
        let mut bytes = Vec::new();
        (&record).take(identity.size + 1).read_to_end(&mut bytes)?;
        ensure!(
            Identity::of(&record)? == identity && bytes.len() as u64 == identity.size,
            "source run binding changed"
        );
        let record: super::super::store::RunRecord = serde_json::from_slice(&bytes)?;
        ensure!(record.ci_source_id == source, "source run binding mismatch");
        Ok(format!("{binding}:{:x}", Sha256::digest(&bytes)))
    }

    pub(super) fn check_anchor(&self) -> Result<()> {
        self.current()?;
        // Walk every configured ancestor without following a symlink. This
        // also rechecks root identity after a state-parent replacement.
        ensure!(
            self.path.is_absolute(),
            "source deletion requires absolute root"
        );
        let mut anchor: File = rustix::fs::open("/", DIR, rustix::fs::Mode::empty())?.into();
        for component in self.path.components() {
            match component {
                std::path::Component::RootDir => {}
                std::path::Component::Normal(name) => {
                    anchor =
                        rustix::fs::openat(&anchor, name, DIR, rustix::fs::Mode::empty())?.into();
                }
                _ => anyhow::bail!("noncanonical source deletion root"),
            }
        }
        ensure!(
            directory(&anchor, true)? == directory(&self.root, true)?,
            "source deletion root changed"
        );
        Ok(())
    }
    fn open_claim_parent(&self, object: &str, path: &str) -> Result<OpenedParent> {
        self.check_anchor()?;
        let mut parts = components(object, path)?;
        let leaf = parts.pop().context("source leaf")?;
        let mut parent = self.root.try_clone()?;
        let mut identities = Vec::with_capacity(2);
        for component in parts {
            parent = rustix::fs::openat(&parent, component, DIR, rustix::fs::Mode::empty())?.into();
            identities.push(directory(&parent, false)?);
        }
        Ok((parent, leaf, identities))
    }
    fn proof(&self, object: &str, path: &str, budget: &mut u64) -> Result<Claim> {
        let (claim, verified) = self.examine(object, path, budget)?;
        verified?;
        Ok(claim)
    }
    /// Read the file of `object` in full, as [`Index::proof`] does. The outer
    /// error is a file that cannot be examined (it is kept); the inner one is
    /// a file that was read whole and stably and is not the content its name
    /// says: a positive finding, with the claim of exactly that file.
    fn examine(&self, object: &str, path: &str, budget: &mut u64) -> Result<(Claim, Result<()>)> {
        let (parent, name, parents) = self.open_claim_parent(object, path)?;
        let mut file: File = rustix::fs::openat(
            &parent,
            &name,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?
        .into();
        let leaf = Identity::of(&file)?;
        ensure!(leaf.leaf(), "unsafe source deletion leaf");
        ensure!(
            leaf.size <= *budget,
            "source deletion validation byte budget"
        );
        *budget -= leaf.size;
        let mut bytes = Vec::new();
        (&mut file).take(leaf.size + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 == leaf.size && Identity::of(&file)? == leaf,
            "source changed during validation"
        );
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let verified = (|| -> Result<()> {
            if let Some(hash) = object.strip_prefix("b:") {
                ensure!(digest == hash, "source blob content identity mismatch");
            } else {
                let manifest: local_ci::source::SourceManifest = serde_json::from_slice(&bytes)?;
                ensure!(
                    Some(manifest.source_id.as_str()) == object.strip_prefix("m:"),
                    "source manifest identity mismatch"
                );
                super::super::store::verify_manifest(&manifest)?;
            }
            Ok(())
        })();
        let incarnation = i64::try_from(self.publication(object)?.get())?;
        ensure!(incarnation < i64::MAX, "source incarnation overflow");
        let claim = Claim {
            object: object.into(),
            path: path.into(),
            incarnation,
            store: directory(&self.root, true)?,
            parents,
            leaf,
            digest,
            abandoned: false,
        };
        Ok((claim, verified))
    }
    fn drain_claim(&self, c: &mut Connection, claim: &Claim, budget: &mut u64) -> Result<()> {
        ensure!(
            claim.incarnation > 0 && claim.incarnation < i64::MAX,
            "source incarnation overflow"
        );
        ensure!(
            directory(&self.root, true)? == claim.store,
            "foreign source deletion claim"
        );
        let current: Option<i64> = c
            .query_row(
                "SELECT incarnation FROM publication WHERE object=?1 AND state='claimed'",
                [&claim.object],
                |r| r.get(0),
            )
            .optional()?;
        ensure!(
            current == Some(claim.incarnation),
            "stale source deletion incarnation"
        );
        let protected:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM member WHERE object=?1) OR EXISTS(SELECT 1 FROM root WHERE source IS NULL AND kind NOT IN ('abandoned','receipt')) OR EXISTS(SELECT 1 FROM graph WHERE done=0)",[&claim.object],|r|r.get(0))?;
        ensure!(!protected, "source deletion claim still protected");
        let (parent, name, parents) = self.open_claim_parent(&claim.object, &claim.path)?;
        ensure!(parents == claim.parents, "source deletion ancestor changed");
        let opened = rustix::fs::openat(
            &parent,
            &name,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        );
        match opened {
            Ok(fd) => {
                let file: File = fd.into();
                ensure!(
                    Identity::of(&file)? == claim.leaf && claim.leaf.leaf(),
                    "source deletion leaf changed"
                );
                ensure!(
                    claim.leaf.size <= *budget,
                    "source replay validation byte budget"
                );
                *budget -= claim.leaf.size;
                let mut digest = Sha256::new();
                let mut reader = (&file).take(claim.leaf.size + 1);
                let bytes = std::io::copy(&mut reader, &mut digest)?;
                ensure!(
                    bytes == claim.leaf.size
                        && format!("{:x}", digest.finalize()) == claim.digest
                        && Identity::of(&file)? == claim.leaf,
                    "source replay content changed"
                );
                self.current()?;
                let (_, _, current_parents) = self.open_claim_parent(&claim.object, &claim.path)?;
                ensure!(
                    current_parents == claim.parents,
                    "source deletion ancestor swapped"
                );
                let named =
                    rustix::fs::statat(&parent, &name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)?;
                // st_dev is platform-dependent (signed on macOS, u64 on Linux).
                #[allow(clippy::unnecessary_cast)]
                let named_device = named.st_dev as u64;
                ensure!(
                    (named_device, named.st_ino) == (claim.leaf.dev, claim.leaf.ino),
                    "source deletion leaf swapped"
                );
                ensure!(
                    Identity::of(&file)? == claim.leaf,
                    "source deletion metadata changed before unlink"
                );
                // The held descriptors and final pathname identity are checked
                // immediately before unlink. No directory is ever removed.
                fault("before_unlink")?;
                rustix::fs::unlinkat(&parent, &name, rustix::fs::AtFlags::empty())?;
                fault("after_unlink")?;
            }
            Err(rustix::io::Errno::NOENT) => {} // Only this durable exact claim permits absence.
            Err(error) => return Err(error.into()),
        }
        parent.sync_all()?;
        fault("after_fsync")?;
        let tx = c.transaction()?;
        let changed=tx.execute("UPDATE publication SET state='live',incarnation=incarnation+1 WHERE object=?1 AND incarnation=?2 AND state='claimed'",params![claim.object,claim.incarnation])?;
        ensure!(changed == 1, "source claim completion changed");
        tx.execute(
            "UPDATE source_delete_claim SET done=1 WHERE object=?1 AND incarnation=?2 AND done=0",
            params![claim.object, claim.incarnation],
        )?;
        fault("before_completion")?;
        tx.commit()?;
        Ok(())
    }
}
/// What became of one membership of a root that is being released.
pub(super) enum Released {
    /// Another root holds the object: only the membership was removed. The
    /// file stays; `true` when it is a reservation's and was read and found
    /// not to be its content (for `doctor`).
    Shared(bool),
    /// The file could not be proven: the membership and the file stay.
    Unproven(String),
    /// The claim is durable and the membership removed; the result of the
    /// unlink follows (a failed one is replayed from the claim).
    Claimed(Result<()>),
}
impl Index {
    /// Release the membership `object` of `root`, which the caller proved is
    /// no longer needed, under source EX: an object another root holds only
    /// loses this membership; a file no other root holds is proven, claimed
    /// durably and unlinked. Half of `budget` remains for the revalidation
    /// of the unlink.
    ///
    /// A `reservation` (a member of a root that never got its source) may
    /// name a file its capture did not install: one that was there with
    /// other content and stopped the install (spec 25.4 (b)). Such a file,
    /// read whole and stably and found not to be its content, is claimed as
    /// well when no other root holds it; one another root holds stays.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn release_member(
        &self,
        c: &mut Connection,
        root: &str,
        object: &str,
        path: &str,
        budget: &mut u64,
        abandoned: bool,
        reservation: bool,
    ) -> Result<Released> {
        let shared: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM member WHERE object=?1 AND root<>?2)",
            params![object, root],
            |r| r.get(0),
        )?;
        if shared {
            let half = *budget / 2;
            let mut look = half;
            let corrupt =
                reservation && matches!(self.examine(object, path, &mut look), Ok((_, Err(_))));
            *budget -= half - look;
            c.execute(
                "DELETE FROM member WHERE root=?1 AND object=?2",
                params![root, object],
            )?;
            return Ok(Released::Shared(corrupt));
        }
        // Half the page budget remains for immediate content revalidation.
        let mut proof_budget = *budget / 2;
        let examined = if reservation {
            self.examine(object, path, &mut proof_budget)
                .map(|(claim, _)| claim)
        } else {
            self.proof(object, path, &mut proof_budget)
        };
        let claim = match examined {
            Ok(claim) => Claim { abandoned, ..claim },
            Err(error) => return Ok(Released::Unproven(format!("{error:#}"))),
        };
        *budget -= claim.leaf.size;
        let tx = c.transaction()?;
        tx.execute("INSERT INTO source_delete_claim(object,incarnation,claim_json,done) VALUES(?1,?2,?3,0)",params![object,claim.incarnation,serde_json::to_string(&claim)?])?;
        tx.execute("UPDATE publication SET state='claimed' WHERE object=?1 AND incarnation=?2 AND state='live'",params![object,claim.incarnation])?;
        tx.execute(
            "DELETE FROM member WHERE root=?1 AND object=?2",
            params![root, object],
        )?;
        tx.commit()?;
        fault("after_claim")?;
        Ok(Released::Claimed(self.drain_claim(c, &claim, budget)))
    }
}
impl SourceStore {
    /// Explicit parent-only page. Samples must come from trusted platform clocks
    /// in the resident driver; callers must retain the owner through actual drain.
    /// Eligibility scans a run's generation history and is not constant-time.
    #[allow(clippy::too_many_arguments)]
    pub fn retention_page(
        &self,
        owner: &crate::storage::CoordinatorStore,
        cursor: &mut RetentionCursor,
        wall: u64,
        mono: u64,
        boot: Option<String>,
        retention: u64,
    ) -> Result<RetentionPage> {
        let _guard = match self.retention.guard(true) {
            Ok(guard) => guard,
            Err(error)
                if error.chain().any(|cause| {
                    cause
                        .downcast_ref::<io::Error>()
                        .is_some_and(|error| error.kind() == io::ErrorKind::WouldBlock)
                }) =>
            {
                return Ok(RetentionPage {
                    busy: true,
                    ..Default::default()
                })
            }
            Err(error) => return Err(error),
        };
        #[cfg(test)]
        self.retention.pause_page();
        self.retention.check_anchor()?;
        // Spec 25.4 (a): the exclusive mutation lock is held, so nothing is
        // writing in `tmp/`; and `secrets.lock` is tried without waiting.
        self.sweep_tmp_dir();
        self.sweep_secret_tmp_if_free();
        let owner_identity = owner.check_input_store(&self.retention.path, &self.retention.root)?;
        let mut c = self.retention.connect()?;
        let registered_owner: Option<String> = c
            .query_row(
                "SELECT value FROM meta WHERE key='retention_owner'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        ensure!(
            registered_owner
                .as_deref()
                .is_none_or(|old| old == owner_identity),
            "source retention owner identity changed"
        );
        c.execute(
            "INSERT OR IGNORE INTO meta VALUES('retention_owner',?1)",
            [&owner_identity],
        )?;
        let previous: Option<String> = c
            .query_row("SELECT value FROM meta WHERE key='clock'", [], |r| r.get(0))
            .optional()?;
        let previous = previous
            .as_deref()
            .map(serde_json::from_str::<crate::results::clock_policy::ClockState>)
            .transpose()?;
        let step = crate::results::clock_policy::step(
            previous.as_ref(),
            boot.as_deref(),
            wall,
            mono,
            retention,
        );
        if let Some(state) = step.state {
            c.execute("INSERT INTO meta VALUES('clock',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[serde_json::to_string(&state)?])?;
        }
        let Some(now) = step.cutoff else {
            return Ok(RetentionPage {
                held: true,
                ..Default::default()
            });
        };
        let unknown:bool=c.query_row("SELECT (SELECT value FROM meta WHERE key='legacy_registration')<>'done' OR EXISTS(SELECT 1 FROM root WHERE source IS NULL AND kind NOT IN ('abandoned','receipt')) OR EXISTS(SELECT 1 FROM graph WHERE done=0)",[],|r|r.get(0))?;
        if unknown {
            return Ok(RetentionPage {
                held: true,
                ..Default::default()
            });
        }
        let pending: Vec<String> = {
            let mut s=c.prepare("SELECT claim_json FROM source_delete_claim WHERE done=0 ORDER BY object,incarnation LIMIT 32")?;
            let rows = s.query_map([], |r| r.get(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        let mut report = RetentionPage::default();
        let mut budget = BYTE_BUDGET;
        if !pending.is_empty() {
            for raw in pending {
                report.paths += 1;
                let claim: Claim = serde_json::from_str(&raw)?;
                match self.retention.drain_claim(&mut c, &claim, &mut budget) {
                    Ok(()) => report.removed += 1,
                    Err(error) => {
                        report.unknown += 1;
                        report.errors.push(format!("{error:#}"));
                    }
                }
            }
            return Ok(report);
        }
        let run:Option<(String,String)>=c.query_row("SELECT id,source FROM root WHERE kind='run' AND id>coalesce(?1,'') ORDER BY id LIMIT 1",[&cursor.run],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let Some((run, source)) = run else {
            *cursor = RetentionCursor::default();
            return Ok(report);
        };
        report.run = Some(run.clone());
        let parsed = crate::ids::RunId::parse(&run)?;
        let source_id =
            super::super::classify::source_id_of_ci(&source).context("invalid owned run source")?;
        let expired: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM run_expiry WHERE run_id=?1 AND source_id=?2)",
            params![run, source],
            |r| r.get(0),
        )?;
        if !expired {
            if cursor
                .validated
                .as_ref()
                .is_none_or(|v| v.manifest.source_id != source)
            {
                cursor.validated = Some(self.retention.load_validated(&source, BYTE_BUDGET)?);
            }
            let validated = cursor.validated.as_ref().unwrap();
            validated.check(&self.retention)?;
            // A run whose record cannot be read keeps its inputs and is
            // counted; the runs after it are still examined, and it is read
            // again on the next pass.
            let bound = match owner.input_retention_binding(
                &parsed,
                &source_id,
                &validated.manifest.manifest_digest,
            ) {
                Ok(bound) => bound,
                Err(error) => {
                    report.unknown += 1;
                    report.errors.push(format!("{error:#}"));
                    None
                }
            };
            let Some((ended, binding)) = bound else {
                cursor.run = Some(run);
                cursor.object.clear();
                cursor.validated = None;
                return Ok(report);
            };
            if ended > now || now - ended < retention {
                cursor.run = Some(run);
                cursor.object.clear();
                cursor.validated = None;
                return Ok(report);
            }
            let binding = self.retention.exact_run_binding(&run, &source, &binding)?;
            // Secret manifests remain roots until this exact run's Vault work
            // has completed. C may be held, busy, behind its cursor, or failed
            // after its durable claim; scheduling order is not authority.
            if !validated.manifest.secret_files.is_empty() {
                let resolved: bool = c.query_row(
                    "SELECT EXISTS(SELECT 1 FROM vault_delete_claim WHERE run_id=?1 AND source_id=?2 AND binding_digest=?3 AND done=1)",
                    params![run, source, binding],
                    |r| r.get(0),
                )?;
                if !resolved {
                    cursor.run = Some(run);
                    cursor.object.clear();
                    cursor.validated = None;
                    return Ok(report);
                }
            }
            // Permanent fence commits BEFORE any membership can be released.
            ensure!(
                owner.check_input_store(&self.retention.path, &self.retention.root)?
                    == owner_identity,
                "source retention owner changed before expiry"
            );
            fault("before_expiry")?;
            c.execute(
                "INSERT INTO run_expiry VALUES(?1,?2,?3)",
                params![run, source, binding],
            )?;
            fault("after_expiry")?;
        }
        let objects: Vec<(String, String)> = {
            let mut s=c.prepare("SELECT m.object,o.path FROM member m JOIN object o ON o.id=m.object WHERE m.root=?1 AND m.object>?2 ORDER BY m.object LIMIT 32")?;
            let rows = s.query_map(params![run, cursor.object], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        report.examined = objects.len() as u32;
        for (object, path) in &objects {
            cursor.object = object.clone();
            match self.retention.release_member(
                &mut c,
                &run,
                object,
                path,
                &mut budget,
                false,
                false,
            )? {
                Released::Shared(_) => report.shared += 1,
                Released::Unproven(error) => {
                    report.unknown += 1;
                    report.errors.push(error);
                }
                Released::Claimed(drained) => {
                    report.claimed += 1;
                    report.paths += 1;
                    match drained {
                        Ok(()) => report.removed += 1,
                        Err(error) => {
                            report.unknown += 1;
                            report.errors.push(format!("{error:#}"));
                        }
                    }
                }
            }
        }
        if objects.len() < PAGE {
            cursor.run = Some(run);
            cursor.object.clear();
            cursor.validated = None;
        }
        Ok(report)
    }
}

#[cfg(test)]
thread_local! { static FAULT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) }; }
#[cfg(test)]
pub(super) fn set_fault(stage: Option<&'static str>) {
    FAULT.set(stage);
}
fn fault(stage: &str) -> Result<()> {
    #[cfg(test)]
    if FAULT.get() == Some(stage) {
        anyhow::bail!("injected {stage}")
    }
    let _ = stage;
    Ok(())
}
