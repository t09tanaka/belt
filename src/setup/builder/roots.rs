//! Spec 25.7 (b): the references to the toolchain roots and their removal.
//!
//! Who reads or writes a root holds a row in `toolchain_ref` of the worker
//! DB for as long as it may: an attempt or a warm-up from its bind (before
//! `receipt.json` is read) to its end; a builder from before it starts
//! (a tool installer before its helper runs, a setup builder with the roots
//! of its execution binding) until its directory is removed, which happens
//! only after its supervisor and everything it started (the install helper
//! is a node of that supervisor, in a process group the supervisor recorded)
//! are confirmed stopped (`supervised` with `SETTLED`, `settle_in_process`,
//! `reap`, `settle_old`); a template made on a root as long as the template
//! is there.
//!
//! A pass removes at most one root: one not referenced, not used for
//! [`ROOT_MAX_AGE_MS`], or, while the measured roots are more than
//! [`ROOT_MAX_BYTES`], the least recently used. It claims the removal in one
//! transaction with the check (`evicting`), then renames the root into the
//! trash and removes it, and forgets the row; a claim left by a crash is
//! finished by the next pass.

use super::*;

/// Unused this long, a root goes (the number of the setup templates: both are
/// a cache that makes the next run faster).
pub(crate) const ROOT_MAX_AGE_MS: u64 = 14 * 24 * 3600 * 1000;
/// Above this many bytes of roots, the least recently used unreferenced one goes.
pub(crate) const ROOT_MAX_BYTES: u64 = 20 << 30;
/// The agent's tick runs a pass at most this often (spec 25.11).
const PASS_EVERY_MS: u64 = 60_000;
/// How long a tool builder waits for a removal of its root to finish.
const EVICTION_WAIT_MS: u64 = 60_000;

/// What a pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RootPass {
    pub removed: Vec<String>,
    /// Why nothing was removed, if a gate stopped the pass.
    pub held: Option<&'static str>,
}

/// The roots a binding runs from: every path component of its environment
/// that is a root directly inside `toolchains`.
pub(crate) fn roots_of(binding: &ExecutionBinding, toolchains: &Path) -> Vec<String> {
    roots_in(&binding.activation.environment, toolchains)
}

fn roots_in(environment: &BTreeMap<String, String>, toolchains: &Path) -> Vec<String> {
    let mut found = BTreeSet::new();
    for value in environment.values() {
        for entry in std::env::split_paths(std::ffi::OsStr::new(value)) {
            let Ok(rest) = entry.strip_prefix(toolchains) else {
                continue;
            };
            if let Some(name) = rest.components().next() {
                let name = name.as_os_str().to_string_lossy();
                if is_identity(&name) {
                    found.insert(name.into_owned());
                }
            }
        }
    }
    found.into_iter().collect()
}

fn is_identity(name: &str) -> bool {
    name.len() == 64 && name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

impl Setups {
    pub(super) fn toolchains(&self) -> PathBuf {
        self.cfg.state_dir.join("toolchains")
    }

    /// At the open of the setup area, under `agent.lock`: give the roots
    /// installed before these tables a row (once), let go of the warm-ups of
    /// an earlier start (they lived in that agent's memory), and run a pass
    /// (which finishes a claimed removal).
    /// The worker DB, opened (and the roots from before given rows, the
    /// warm-ups of an earlier start let go) at the first use: the agent's
    /// first tick or the first bind, whichever comes first.
    pub(super) fn db(&self) -> Option<&WorkerStore> {
        self.roots
            .get_or_init(|| {
                let db = WorkerStore::open(&self.cfg.state_dir, &self.cfg.worker_id).ok()?;
                self.open_roots(&db);
                Some(db)
            })
            .as_ref()
    }

    fn open_roots(&self, db: &WorkerStore) {
        if !db.toolchain_roots_seeded().unwrap_or(true) {
            let found = std::fs::read_dir(self.toolchains())
                .map(|rd| {
                    rd.flatten()
                        .filter_map(|e| e.file_name().into_string().ok())
                        .filter(|n| is_identity(n))
                        .filter(|n| {
                            std::fs::symlink_metadata(
                                self.toolchains().join(n).join("receipt.json"),
                            )
                            .is_ok_and(|m| m.is_file())
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let _ = db.seed_toolchain_roots(found, platform::wall_ms());
        }
        let _ = db.toolchain_release_kind("warmup");
    }

    /// The bind of `consumer` to the root of `plan`, before its receipt is
    /// read. Without the worker DB nothing can remove a root either, and the
    /// root is used as before.
    pub(super) fn bind_root(&self, consumer: &AttemptId, root: &Path) -> bool {
        let Some(db) = self.db() else {
            return true;
        };
        let Some(identity) = root.file_name().and_then(|n| n.to_str()) else {
            return false;
        };
        matches!(
            db.toolchain_bind(identity, consumer.as_str(), platform::wall_ms()),
            Ok(crate::storage::Bind::Bound)
        )
    }

    /// A tool builder references the root it installs into before its helper
    /// starts; while the root is being removed it waits (bounded).
    pub(super) fn hold_root_for_install(&self, id: &BuilderId, root: &Path) -> Result<(), String> {
        let Some(db) = self.db() else {
            return Ok(());
        };
        let identity = root
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or("tool root has no name")?;
        let begun = platform::monotonic_ms();
        loop {
            match db.toolchain_hold_for_install(identity, id.as_str()) {
                Ok(true) => return Ok(()),
                Ok(false) if platform::monotonic_ms().saturating_sub(begun) < EVICTION_WAIT_MS => {
                    if self.wanted(id).0 == 0 {
                        return Err("tool preparation cancelled".into());
                    }
                    std::thread::sleep(Duration::from_millis(self.cfg.poll_ms.clamp(1, 100)));
                }
                Ok(false) => return Err("the toolchain root is being removed; try again".into()),
                Err(e) => return Err(e.to_string()),
            }
        }
    }

    pub(super) fn root_installed(&self, root: &Path) {
        if let (Some(db), Some(identity)) = (self.db(), root.file_name().and_then(|n| n.to_str())) {
            let _ = db.toolchain_installed(identity, platform::wall_ms());
        }
    }

    pub(super) fn add_root_refs(&self, identities: Vec<String>, kind: &'static str, holder: &str) {
        if let Some(db) = self.db() {
            if !identities.is_empty() {
                let _ = db.toolchain_add_refs(identities, kind, holder);
            }
        }
    }

    /// The builder's directory is gone (so is everything it started): its
    /// references go.
    pub(super) fn release_builder_roots(&self, id: &BuilderId) {
        if let Some(db) = self.db() {
            let _ = db.toolchain_release("builder", id.as_str());
        }
    }

    pub(super) fn release_warmup_roots(&self, consumer: &AttemptId) {
        if let Some(db) = self.db() {
            let _ = db.toolchain_release("warmup", consumer.as_str());
        }
    }

    /// From the agent's tick, at most every [`PASS_EVERY_MS`].
    /// The pass runs on its own thread, one at a time: measuring or removing
    /// a large root never holds up the tick (or a stop that waits for it).
    pub fn roots_tick(self: &Arc<Self>) {
        let now = platform::monotonic_ms();
        let last = self.last_roots_pass.load(Ordering::SeqCst);
        if last != 0 && now.saturating_sub(last) < PASS_EVERY_MS {
            return;
        }
        if self.roots_passing.swap(true, Ordering::SeqCst) {
            return;
        }
        self.last_roots_pass.store(now.max(1), Ordering::SeqCst);
        let me = self.clone();
        let spawned = std::thread::Builder::new()
            .name("toolchain-roots".into())
            .spawn(move || {
                me.roots_pass(platform::wall_ms());
                me.roots_passing.store(false, Ordering::SeqCst);
            });
        if spawned.is_err() {
            self.roots_passing.store(false, Ordering::SeqCst);
        }
    }

    /// One pass (see the module): release what is provably over, then
    /// remove at most one root.
    pub(crate) fn roots_pass(&self, now_ms: u64) -> RootPass {
        let mut pass = RootPass::default();
        let Some(db) = self.db() else {
            pass.held = Some("no_database");
            return pass;
        };
        // 25.1 principle 8: nothing is removed while an upgrade is on trial.
        if crate::worker::control::in_trial(&self.cfg.state_dir) {
            pass.held = Some("trial");
            return pass;
        }
        // References whose holder is provably gone: a builder whose directory
        // is removed, a template that is no longer in the store.
        for holder in db.toolchain_holders("builder").unwrap_or_default() {
            if !self.cfg.builders().join(&holder).exists() {
                let _ = db.toolchain_release("builder", &holder);
            }
        }
        for holder in db.toolchain_holders("template").unwrap_or_default() {
            if !self.store.holds(&holder) {
                let _ = db.toolchain_release("template", &holder);
            }
        }
        // An attempt accepted before the first start with these tables holds
        // no reference to the roots it runs from: nothing goes until it ends.
        if db.toolchain_attempt_from_before().unwrap_or(true) {
            pass.held = Some("attempt_from_before");
            return pass;
        }
        if lock(&self.inner).unknown_builder {
            pass.held = Some("unknown_builder");
            return pass;
        }
        // A builder directory with no reference row may be one of a version
        // that records none (or one of this version before its first row, or
        // a setup builder with no toolchain): it may use any root, so nothing
        // goes while one is there. A directory is removed only once its
        // builder and what that started are settled.
        let referenced: BTreeSet<String> = db
            .toolchain_holders("builder")
            .unwrap_or_default()
            .into_iter()
            .collect();
        let unrecorded = std::fs::read_dir(self.cfg.builders())
            .map(|rd| {
                rd.flatten()
                    .any(|e| !referenced.contains(e.file_name().to_string_lossy().as_ref()))
            })
            .unwrap_or(true);
        if unrecorded {
            pass.held = Some("builder_without_references");
            return pass;
        }
        let Ok(rows) = db.toolchain_roots() else {
            pass.held = Some("database");
            return pass;
        };
        // A claim a crash left, first.
        if let Some(row) = rows.iter().find(|r| r.evicting) {
            if self.evict_root(db, &row.identity) {
                pass.removed.push(row.identity.clone());
            }
            return pass;
        }
        // 14 days unused (a clock that went back makes nothing old).
        let old = now_ms.checked_sub(ROOT_MAX_AGE_MS);
        if let Some(before) = old {
            for row in rows.iter().filter(|r| r.last_used_ms < before) {
                if db
                    .toolchain_claim(&row.identity, Some(before))
                    .unwrap_or(false)
                {
                    if self.evict_root(db, &row.identity) {
                        pass.removed.push(row.identity.clone());
                    }
                    return pass;
                }
            }
        }
        // Over the size: measure one unmeasured root a pass, and judge by
        // what is measured (an unmeasured root counts nothing).
        if let Some(row) = rows.iter().find(|r| r.bytes.is_none()) {
            if let Ok(bytes) = adapters::root_bytes(&self.toolchains().join(&row.identity)) {
                let _ = db.toolchain_set_bytes(&row.identity, bytes);
            }
        }
        let Ok(rows) = db.toolchain_roots() else {
            return pass;
        };
        let total: u64 = rows.iter().filter_map(|r| r.bytes).sum();
        if total > ROOT_MAX_BYTES {
            for row in &rows {
                if db.toolchain_claim(&row.identity, None).unwrap_or(false) {
                    if self.evict_root(db, &row.identity) {
                        pass.removed.push(row.identity.clone());
                    }
                    return pass;
                }
            }
        }
        pass
    }

    /// Remove a claimed root and forget its row. `false` when the root could
    /// not be moved now (the claim stays and the next pass tries again).
    fn evict_root(&self, db: &WorkerStore, identity: &str) -> bool {
        match adapters::evict_root(&self.toolchains(), identity) {
            Ok(()) => db.toolchain_forget(identity).is_ok(),
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests;
