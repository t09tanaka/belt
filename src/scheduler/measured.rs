//! Parent-owned measurements. Lookup never opens storage or mutates history.
//! Transport costs are deliberately distinct from worker runtime/profile history.
mod preparation;
pub use preparation::PreparationSample;
enum Write {
    Phase(Sample),
    Preparation(Box<PreparationSample>),
}
use super::decide::Costs;
use crate::ids::{AgentEpoch, Digest, WorkerBootId, WorkerId};
use crate::platform;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::Duration;

const CAP: usize = 4096;
const QUEUE: usize = 32;
pub(crate) const TTL_MS: u64 = 24 * 60 * 60 * 1000;

/// Positive connection identity, never inferred from a worker name alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transport {
    pub worker_boot: WorkerBootId,
    pub agent_epoch: AgentEpoch,
    /// Versioned effective connection policy; not a worker toolchain fingerprint.
    pub policy: String,
}
#[derive(Debug, Clone, Default)]
pub struct CostQuery {
    pub transport: Option<Transport>,
    pub transfer_binding: Option<String>,
    pub collection_binding: Option<String>,
    /// Supplied only after a current gate proves run/source/manifest readiness.
    pub ready: bool,
    pub collection_policy: Option<String>,
}
impl CostQuery {
    pub fn task(spec: &Value, source: Option<&str>, transport: Option<Transport>) -> Self {
        let transfer_binding = source.and_then(|source| {
            Some(hash(&serde_json::json!({"source":source,
                "manifest":spec.get("manifest_digest")?,
                "secret":spec.get("has_secret_bundle").and_then(Value::as_bool).unwrap_or(false),
                "in_place":spec.get("in_place").and_then(Value::as_bool).unwrap_or(false),
                "policy":"verified-source-v1"})))
        });
        // Exact commands/output policy, including environment and declared paths.
        // Unknown future task fields cannot accidentally mean no setup: setup is
        // always Unknown independently of this network-only binding.
        let collection_binding = transfer_binding.as_ref().and_then(|source| {
            Some(hash(
                &serde_json::json!({"source":source,"commands":spec.get("commands")?,
                "artifacts":spec.get("artifacts").cloned().unwrap_or_else(||serde_json::json!([])),
                "outputs":spec.get("outputs").cloned().unwrap_or_else(||serde_json::json!([])),
                "setup":spec.get("setup").unwrap_or(&Value::Null),
                "toolchains":spec.get("toolchains").unwrap_or(&Value::Null),
                "requirements":spec.get("requirements").unwrap_or(&Value::Null),
                "command_timeout_ms":spec.get("command_timeout_ms").unwrap_or(&Value::Null),
                "deadline_ms":spec.get("deadline_ms").unwrap_or(&Value::Null),
                "resource_wait_timeout_ms":spec.get("resource_wait_timeout_ms").unwrap_or(&Value::Null),
                "policy":"post-terminal-tail-streaming-v1"}),
            ))
        });
        Self {
            transport,
            transfer_binding,
            collection_binding,
            ready: false,
            collection_policy: None,
        }
    }
}
fn hash(value: &Value) -> String {
    Digest::of_bytes(&serde_json::to_vec(value).expect("JSON value serializes")).to_string()
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Phase {
    Transfer,
    TerminalTail,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    key: String,
    receipt: String,
    boot: String,
    observed: u64,
    duration: u64,
}
impl Sample {
    pub fn verified(
        worker: &WorkerId,
        query: &CostQuery,
        phase: Phase,
        receipt: String,
        started: u64,
        duration: u64,
    ) -> Option<Self> {
        let boot = platform::boot_id()?;
        let now = platform::monotonic_ms();
        if now == u64::MAX
            || started == u64::MAX
            || duration == u64::MAX
            || started.checked_add(duration)? > now
        {
            return None;
        }
        Some(Self {
            key: key(worker, query, phase)?,
            receipt: format!("{receipt}:{boot}"),
            boot,
            observed: started,
            duration,
        })
    }
}
fn key(worker: &WorkerId, query: &CostQuery, phase: Phase) -> Option<String> {
    let binding = match phase {
        Phase::Transfer => query.transfer_binding.as_ref()?,
        Phase::TerminalTail => query.collection_binding.as_ref()?,
    };
    Some(hash(
        &serde_json::json!({"version":1,"worker":worker,"transport":query.transport.as_ref()?,"phase":phase,"binding":binding,"collection_policy":match phase {Phase::Transfer=>None,Phase::TerminalTail=>query.collection_policy.as_ref()}}),
    ))
}

/// Parent-only cost evidence; this is not worker runtime-history provenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    Measured,
    Fallback,
    Unknown,
    VerifiedReady,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    pub worker_id: WorkerId,
    pub worker_boot: WorkerBootId,
    pub agent_epoch: AgentEpoch,
    pub parent_boot: String,
    pub transport_policy_digest: String,
    pub binding_digest: String,
    pub collection_policy_digest: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentEvidence {
    pub kind: EvidenceKind,
    pub ms: Option<u64>,
    pub sample_count: Option<u32>,
    pub observed_monotonic_ms: Option<u64>,
    pub remaining_ttl_ms: Option<u64>,
    pub provenance: Option<Provenance>,
    pub reason: Option<String>,
}
impl ComponentEvidence {
    fn unknown(reason: &str) -> Self {
        Self {
            kind: EvidenceKind::Unknown,
            ms: None,
            sample_count: None,
            observed_monotonic_ms: None,
            remaining_ttl_ms: None,
            provenance: None,
            reason: Some(reason.into()),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCostEvidence {
    pub worker_id: WorkerId,
    pub costs: CostEvidence,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEvidence {
    pub transfer: ComponentEvidence,
    pub setup: ComponentEvidence,
    pub collection: ComponentEvidence,
}
impl CostEvidence {
    pub(super) fn unknown(reason: &str) -> Self {
        Self {
            transfer: ComponentEvidence::unknown(reason),
            setup: ComponentEvidence::unknown(reason),
            collection: ComponentEvidence::unknown(reason),
        }
    }
    pub fn fallback(&mut self, costs: &Costs) {
        for (part, component) in [&mut self.transfer, &mut self.setup, &mut self.collection]
            .into_iter()
            .enumerate()
        {
            if costs.assumed[part] {
                component.kind = EvidenceKind::Fallback;
                component.ms = costs.ms[part];
                component.reason = Some(
                    "worst known candidate cost; ranking assumption, not a sample from this worker"
                        .into(),
                );
            }
        }
    }
}

/// The writer owns SQLite; the scheduler owns only this bounded snapshot.
/// Only committed samples are published. A full queue drops optional telemetry.
pub struct MeasuredCosts {
    boot: Option<String>,
    policy: String,
    cache: Arc<RwLock<BTreeMap<String, Sample>>>,
    writer: Mutex<Option<mpsc::SyncSender<Write>>>,
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl MeasuredCosts {
    pub fn open(stats: &Path, cfg: crate::results::CollectConfig) -> anyhow::Result<Self> {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
        crate::service::resident::validate_components(stats)?;
        if !stats.exists() {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(stats)?;
        }
        let metadata = std::fs::symlink_metadata(stats)?;
        anyhow::ensure!(
            metadata.is_dir()
                && metadata.uid() == rustix::process::geteuid().as_raw()
                && metadata.mode() & 0o077 == 0,
            "unsafe cost statistics directory"
        );
        let path = stats.join("costs-v1.sqlite");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)?;
        let metadata = file.metadata()?;
        anyhow::ensure!(
            metadata.is_file()
                && metadata.nlink() == 1
                && metadata.uid() == rustix::process::geteuid().as_raw()
                && metadata.mode() & 0o077 == 0,
            "unsafe cost statistics file"
        );
        for suffix in ["costs-v1.sqlite-wal", "costs-v1.sqlite-shm"] {
            if let Ok(metadata) = std::fs::symlink_metadata(stats.join(suffix)) {
                anyhow::ensure!(
                    metadata.is_file()
                        && !metadata.file_type().is_symlink()
                        && metadata.nlink() == 1
                        && metadata.uid() == rustix::process::geteuid().as_raw()
                        && metadata.mode() & 0o077 == 0,
                    "unsafe statistics sidecar"
                );
            }
        }
        let db = Connection::open(stats.join("costs-v1.sqlite"))?;
        db.busy_timeout(Duration::from_millis(10))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS costs(version INTEGER NOT NULL, key TEXT PRIMARY KEY, receipt TEXT NOT NULL UNIQUE, boot TEXT NOT NULL, observed INTEGER NOT NULL, duration INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS costs_age ON costs(observed,key);
            CREATE TABLE IF NOT EXISTS cost_eviction(id INTEGER PRIMARY KEY CHECK(id=1),boot TEXT NOT NULL,cutoff INTEGER NOT NULL);")?;
        preparation::initialize(&db)?;
        let mut cache = BTreeMap::new();
        {
            let mut stmt = db.prepare("SELECT key,receipt,boot,observed,duration FROM costs WHERE version IN (1,2) ORDER BY observed DESC LIMIT ?1")?;
            for row in stmt.query_map([CAP as i64], |r| {
                Ok(Sample {
                    key: r.get(0)?,
                    receipt: r.get(1)?,
                    boot: r.get(2)?,
                    observed: read_u64(r, 3)?,
                    duration: read_u64(r, 4)?,
                })
            })? {
                let sample = row?;
                cache.insert(sample.key.clone(), sample);
            }
        }
        let cache = Arc::new(RwLock::new(cache));
        let (tx, rx) = mpsc::sync_channel::<Write>(QUEUE);
        let view = cache.clone();
        let join = std::thread::Builder::new()
            .name("parent-cost-writer".into())
            .spawn(move || {
                let mut db = db;
                while let Ok(message) = rx.recv() {
                    let (sample, committed) = match message {
                        Write::Phase(sample) => {
                            let committed = record(&mut db, &sample).unwrap_or(false);
                            (sample, committed)
                        }
                        Write::Preparation(value) => {
                            let committed = preparation::record(&mut db, &value).unwrap_or(false);
                            (value.sample, committed)
                        }
                    };
                    if !committed {
                        continue;
                    }
                    let mut cache = view.write().unwrap_or_else(|e| e.into_inner());
                    // Keep the original observation on a repeated receipt.
                    if cache.values().any(|s| s.receipt == sample.receipt) {
                        continue;
                    }
                    let boot = sample.boot.clone();
                    cache.insert(sample.key.clone(), sample);
                    if cache.len() > CAP {
                        if let Some(old) = cache
                            .values()
                            .min_by(|a, b| {
                                (a.boot == boot, a.observed)
                                    .cmp(&(b.boot == boot, b.observed))
                                    .then_with(|| b.key.cmp(&a.key))
                            })
                            .map(|s| s.key.clone())
                        {
                            cache.remove(&old);
                        }
                    }
                }
            })?;
        Ok(Self {
            boot: platform::boot_id(),
            policy: collection_policy(cfg),
            cache,
            writer: Mutex::new(Some(tx)),
            join: Mutex::new(Some(join)),
        })
    }
    pub fn record(&self, sample: Sample) {
        if let Some(tx) = self
            .writer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            let _ = tx.try_send(Write::Phase(sample));
        }
    }
    fn lookup_at(&self, worker: &WorkerId, query: &CostQuery, now: u64) -> Costs {
        let mut costs = Costs::default();
        if now == u64::MAX {
            return costs;
        }
        let Some(boot) = &self.boot else {
            return costs;
        };
        let cache = self.cache.read().unwrap_or_else(|e| e.into_inner());
        for (part, phase) in [(0, Phase::Transfer), (2, Phase::TerminalTail)] {
            if let Some(sample) = key(worker, query, phase).and_then(|k| cache.get(&k)) {
                if sample.boot == *boot
                    && now
                        .checked_sub(sample.observed)
                        .is_some_and(|age| age <= TTL_MS)
                {
                    costs.ms[part] = Some(sample.duration);
                }
            }
        }
        if query.ready && query.transport.is_some() && query.transfer_binding.is_some() {
            costs.ms[0] = Some(0);
            costs.cache_warm = Some(true);
        }
        costs
    }
    fn evidence_at(&self, worker: &WorkerId, query: &CostQuery, now: u64) -> CostEvidence {
        let component = |phase: Phase| {
            let binding = match phase {
                Phase::Transfer => query.transfer_binding.as_ref(),
                Phase::TerminalTail => query.collection_binding.as_ref(),
            };
            let Some(binding) = binding else {
                return ComponentEvidence::unknown(
                    "exact source/output identity unavailable; no source was captured",
                );
            };
            let Some(transport) = &query.transport else {
                return ComponentEvidence::unknown("verified transport identity unavailable");
            };
            let Some(boot) = &self.boot else {
                return ComponentEvidence::unknown("parent boot identity unavailable");
            };
            if now == u64::MAX {
                return ComponentEvidence::unknown("monotonic clock unavailable");
            }
            let provenance = Provenance {
                worker_id: worker.clone(),
                worker_boot: transport.worker_boot.clone(),
                agent_epoch: transport.agent_epoch,
                parent_boot: boot.clone(),
                transport_policy_digest: Digest::of_bytes(transport.policy.as_bytes()).to_string(),
                binding_digest: binding.clone(),
                collection_policy_digest: match phase {
                    Phase::Transfer => None,
                    Phase::TerminalTail => query.collection_policy.clone(),
                },
            };
            if matches!(phase, Phase::Transfer) && query.ready {
                return ComponentEvidence {kind:EvidenceKind::VerifiedReady,ms:Some(0),sample_count:Some(0),observed_monotonic_ms:None,remaining_ttl_ms:None,provenance:Some(provenance),reason:Some("current gate verified exact run/source/manifest readiness; no latency sample asserted".into())};
            }
            let cache = self.cache.read().unwrap_or_else(|e| e.into_inner());
            let Some(sample) = key(worker, query, phase).and_then(|key| cache.get(&key)) else {
                return ComponentEvidence::unknown("no exact verified successful measurement");
            };
            if sample.boot != *boot {
                return ComponentEvidence::unknown(
                    "measurement belongs to a different parent boot",
                );
            }
            let Some(age) = now.checked_sub(sample.observed) else {
                return ComponentEvidence::unknown("monotonic clock precedes observation");
            };
            if age > TTL_MS {
                return ComponentEvidence::unknown("measurement TTL expired");
            }
            ComponentEvidence {
                kind: EvidenceKind::Measured,
                ms: Some(sample.duration),
                sample_count: Some(1),
                observed_monotonic_ms: Some(sample.observed),
                remaining_ttl_ms: Some(TTL_MS - age),
                provenance: Some(provenance),
                reason: Some(
                    "latest single verified successful phase; not a historical mean".into(),
                ),
            }
        };
        CostEvidence {
            transfer: component(Phase::Transfer),
            setup: ComponentEvidence::unknown("verified worker setup timing receipt unavailable"),
            collection: component(Phase::TerminalTail),
        }
    }

    /// Close input then drain the finite queue. Never called from placement.
    pub fn drain(&self) {
        self.writer.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(join) = self.join.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = join.join();
        }
    }
}
fn read_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
}
fn record(db: &mut Connection, sample: &Sample) -> rusqlite::Result<bool> {
    if platform::boot_id().as_deref() != Some(sample.boot.as_str()) {
        return Ok(false);
    }
    let (Ok(observed), Ok(duration)) = (
        i64::try_from(sample.observed),
        i64::try_from(sample.duration),
    ) else {
        return Ok(false);
    };
    let tx = db.transaction()?;
    let cutoff: Option<(String, i64)> = tx
        .query_row(
            "SELECT boot,cutoff FROM cost_eviction WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if cutoff
        .as_ref()
        .is_some_and(|(boot, at)| boot == &sample.boot && observed <= *at)
    {
        return Ok(false);
    }
    if cutoff
        .as_ref()
        .is_some_and(|(boot, _)| boot != &sample.boot)
    {
        tx.execute("DELETE FROM cost_eviction WHERE id=1", [])?;
    }
    // Replays never refresh the original observation. Hooks emit only tracked
    // new phases, so evicted old receipts cannot be re-created by closure scans.
    let changed=tx.execute("INSERT INTO costs(version,key,receipt,boot,observed,duration) VALUES(1,?1,?2,?3,?4,?5) ON CONFLICT(key) DO UPDATE SET receipt=excluded.receipt,boot=excluded.boot,observed=excluded.observed,duration=excluded.duration WHERE costs.receipt != excluded.receipt AND (costs.boot != excluded.boot OR costs.observed < excluded.observed)",
        params![sample.key,sample.receipt,sample.boot,observed,duration])?;
    prune(&tx, &sample.boot)?;
    tx.commit()?;
    Ok(changed > 0)
}
fn prune(tx: &rusqlite::Transaction<'_>, boot: &str) -> rusqlite::Result<()> {
    tx.execute("UPDATE preparation_namespaces SET cutoff=MAX(cutoff,coalesce((SELECT MAX(p.sequence) FROM preparation_receipts p JOIN costs c ON c.key=p.key WHERE p.namespace=preparation_namespaces.namespace AND c.key IN (SELECT key FROM costs ORDER BY (boot=?2) DESC,observed DESC,key LIMIT -1 OFFSET ?1)),0)) WHERE namespace IN (SELECT namespace FROM preparation_receipts WHERE key IN (SELECT key FROM costs ORDER BY (boot=?2) DESC,observed DESC,key LIMIT -1 OFFSET ?1))", params![CAP as i64,boot])?;
    tx.execute("DELETE FROM preparation_receipts WHERE key IN (SELECT key FROM costs ORDER BY (boot=?2) DESC,observed DESC,key LIMIT -1 OFFSET ?1)",params![CAP as i64,boot])?;
    let evicted:Option<i64>=tx.query_row("SELECT MAX(observed) FROM (SELECT boot,observed FROM costs ORDER BY (boot=?2) DESC,observed DESC,key LIMIT -1 OFFSET ?1) WHERE boot=?2",params![CAP as i64,boot],|r|r.get(0))?;
    if let Some(evicted) = evicted {
        tx.execute("INSERT INTO cost_eviction(id,boot,cutoff) VALUES(1,?1,?2) ON CONFLICT(id) DO UPDATE SET boot=excluded.boot,cutoff=CASE WHEN cost_eviction.boot=excluded.boot THEN MAX(cost_eviction.cutoff,excluded.cutoff) ELSE excluded.cutoff END",params![boot,evicted])?;
    }
    tx.execute("DELETE FROM costs WHERE key IN (SELECT key FROM costs ORDER BY (boot=?2) DESC,observed DESC,key LIMIT -1 OFFSET ?1)",params![CAP as i64,boot])?;
    Ok(())
}
impl super::CostModel for MeasuredCosts {
    fn costs(&self, _: &WorkerId, _: &crate::storage::QueuedTask) -> Costs {
        Costs::default()
    }
    fn query(&self, worker: &WorkerId, query: &CostQuery) -> Costs {
        self.lookup_at(worker, query, platform::monotonic_ms())
    }
    fn evidence(&self, worker: &WorkerId, query: &CostQuery) -> Option<CostEvidence> {
        Some(self.evidence_at(worker, query, platform::monotonic_ms()))
    }
    fn record(&self, sample: Sample) {
        self.record(sample);
    }
    fn record_preparation(&self, sample: PreparationSample) {
        self.record_preparation(sample);
    }
    fn drain(&self) {
        self.drain();
    }
    fn collection_policy(&self) -> Option<String> {
        Some(self.policy.clone())
    }
}
impl Drop for MeasuredCosts {
    fn drop(&mut self) {
        self.drain();
    }
}

/// Settings that change active terminal-tail IO are part of its identity.
pub fn collection_policy(c: crate::results::CollectConfig) -> String {
    hash(
        &serde_json::json!({"version":1,"streaming":true,"chunk":c.chunk_bytes,
        "artifact":c.max_artifact_bytes,"log":c.max_log_bytes}),
    )
}
struct Tail {
    worker: WorkerId,
    query: CostQuery,
    observed: u64,
    active: u64,
    running: bool,
}
/// A process-local accumulator. Poisoned entries remain bounded tombstones;
/// reaching the cap disables additional telemetry rather than retracking a tail.
#[derive(Default)]
pub struct Tails {
    entries: Mutex<BTreeMap<String, Option<Tail>>>,
}
pub struct Segment {
    tails: Arc<Tails>,
    attempt: String,
    started: u64,
    finished: bool,
}
impl Tails {
    pub fn begin(
        self: &Arc<Self>,
        attempt: &crate::ids::AttemptId,
        worker: &WorkerId,
        query: CostQuery,
        pristine: bool,
    ) -> Option<Segment> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let id = attempt.to_string();
        let now = platform::monotonic_ms();
        if !entries.contains_key(&id) {
            if entries.len() >= CAP {
                return None;
            }
            let tail = (pristine && now != u64::MAX && query.transport.is_some()).then(|| Tail {
                worker: worker.clone(),
                query: query.clone(),
                observed: now,
                active: 0,
                running: false,
            });
            entries.insert(id.clone(), tail);
        }
        let tail = entries.get_mut(&id)?.as_mut()?;
        if tail.running
            || tail.query.transport != query.transport
            || tail.query.collection_binding != query.collection_binding
            || tail.query.collection_policy != query.collection_policy
            || now < tail.observed
            || now == u64::MAX
        {
            entries.insert(id, None);
            return None;
        }
        tail.running = true;
        Some(Segment {
            tails: self.clone(),
            attempt: id,
            started: now,
            finished: false,
        })
    }
}
impl Segment {
    pub fn finish(mut self, healthy: bool, complete: bool) -> Option<Sample> {
        self.finished = true;
        let now = platform::monotonic_ms();
        let mut entries = self.tails.entries.lock().unwrap_or_else(|e| e.into_inner());
        let entry = entries.get_mut(&self.attempt)?;
        if !healthy || now == u64::MAX || now < self.started {
            *entry = None;
            return None;
        }
        let tail = entry.as_mut()?;
        tail.active = tail.active.checked_add(now - self.started)?;
        tail.running = false;
        if !complete {
            return None;
        }
        let sample = Sample::verified(
            &tail.worker,
            &tail.query,
            Phase::TerminalTail,
            format!("terminal-tail:{}", self.attempt),
            tail.observed,
            tail.active,
        );
        // Only the caller's successful durable Complete close reaches here.
        // Later real scans require a missing CollectionRow to start tracking,
        // so that durable receipt now owns replay exclusion across restarts.
        entries.remove(&self.attempt);
        sample
    }
}
impl Drop for Segment {
    fn drop(&mut self) {
        if !self.finished {
            self.tails
                .entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(self.attempt.clone(), None);
        }
    }
}

#[cfg(test)]
fn private_test_root() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir_in(std::fs::canonicalize(std::env::temp_dir()).unwrap()).unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

#[cfg(test)]
mod tests {
    use super::*;
    fn query() -> (WorkerId, CostQuery) {
        let worker = WorkerId::parse("worker-a").unwrap();
        let spec = serde_json::json!({"manifest_digest":Digest::of_bytes(b"source"),"commands":[{"argv":["true"],"env":{}}],"outputs":[]});
        let transport = Transport {
            worker_boot: WorkerBootId::parse("boot-a").unwrap(),
            agent_epoch: AgentEpoch(1),
            policy: "private-rpc-v1".into(),
        };
        (
            worker,
            CostQuery::task(&spec, Some("source-a"), Some(transport)),
        )
    }
    fn sample(worker: &WorkerId, query: &CostQuery, phase: Phase, receipt: &str) -> Sample {
        let now = platform::monotonic_ms();
        Sample::verified(
            worker,
            query,
            phase,
            receipt.into(),
            now.saturating_sub(10),
            10,
        )
        .unwrap()
    }
    #[test]
    fn durable_same_boot_restart_exact_identity_and_ttl() {
        let dir = private_test_root();
        let (worker, query) = query();
        let model =
            MeasuredCosts::open(dir.path(), crate::results::CollectConfig::default()).unwrap();
        let measurement = sample(&worker, &query, Phase::Transfer, "one");
        let observed = measurement.observed;
        model.record(measurement);
        model.drain();
        assert_eq!(
            model.lookup_at(&worker, &query, observed + 10).ms,
            [Some(10), None, None]
        );
        assert_eq!(model.lookup_at(&worker, &query, observed - 1).ms, [None; 3]);
        assert_eq!(
            model.lookup_at(&worker, &query, observed + TTL_MS + 1).ms,
            [None; 3]
        );
        assert_eq!(model.lookup_at(&worker, &query, u64::MAX).ms, [None; 3]);
        drop(model);
        let mut model =
            MeasuredCosts::open(dir.path(), crate::results::CollectConfig::default()).unwrap();
        assert_eq!(
            model.lookup_at(&worker, &query, observed + 10).ms[0],
            Some(10)
        );
        let mut changed = query.clone();
        changed.transport.as_mut().unwrap().agent_epoch = AgentEpoch(2);
        assert_eq!(
            model.lookup_at(&worker, &changed, observed + 10).ms[0],
            None
        );
        changed = query.clone();
        changed.transfer_binding = Some("different source".into());
        assert_eq!(
            model.lookup_at(&worker, &changed, observed + 10).ms[0],
            None
        );
        changed = query.clone();
        changed.transport.as_mut().unwrap().policy = "other transport".into();
        assert_eq!(
            model.lookup_at(&worker, &changed, observed + 10).ms[0],
            None
        );
        let other = WorkerId::parse("other-worker").unwrap();
        assert_eq!(model.lookup_at(&other, &query, observed + 10).ms[0], None);
        let mut warm = query.clone();
        warm.ready = true;
        assert_eq!(
            model.lookup_at(&worker, &warm, observed + TTL_MS + 1).ms[0],
            Some(0)
        );
        warm.transport = None;
        assert_eq!(model.lookup_at(&worker, &warm, observed + 10).ms[0], None);
        model.boot = Some("previous-boot".into());
        assert_eq!(model.lookup_at(&worker, &query, observed + 10).ms[0], None);
    }
    #[test]
    fn hundred_lookups_do_not_write_and_unknown_is_not_zero() {
        let dir = private_test_root();
        let (worker, mut query) = query();
        let model =
            MeasuredCosts::open(dir.path(), crate::results::CollectConfig::default()).unwrap();
        model.record(sample(&worker, &query, Phase::Transfer, "once"));
        model.drain();
        let db = Connection::open(dir.path().join("costs-v1.sqlite")).unwrap();
        let before: String = db
            .query_row(
                "SELECT group_concat(key||receipt||observed||duration) FROM costs",
                [],
                |r| r.get(0),
            )
            .unwrap();
        for _ in 0..100 {
            assert_eq!(
                model
                    .lookup_at(&worker, &query, platform::monotonic_ms())
                    .ms[0],
                Some(10)
            );
        }
        let after: String = db
            .query_row(
                "SELECT group_concat(key||receipt||observed||duration) FROM costs",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(before, after);
        query.transfer_binding = None;
        assert_eq!(
            model
                .lookup_at(&worker, &query, platform::monotonic_ms())
                .ms,
            [None; 3]
        );
        query.ready = true;
        assert_eq!(
            model
                .lookup_at(&worker, &query, platform::monotonic_ms())
                .ms[0],
            None
        );
    }
    #[test]
    fn record_cap_and_replayed_receipt_preserve_original_observation() {
        let dir = private_test_root();
        let model =
            MeasuredCosts::open(dir.path(), crate::results::CollectConfig::default()).unwrap();
        model.drain();
        let mut db = Connection::open(dir.path().join("costs-v1.sqlite")).unwrap();
        let (worker, query) = query();
        let first = sample(&worker, &query, Phase::Transfer, "one");
        assert!(record(&mut db, &first).unwrap());
        let mut replay = first.clone();
        replay.observed += 100;
        replay.duration = 0;
        assert!(!record(&mut db, &replay).unwrap());
        assert_eq!(
            db.query_row(
                "SELECT duration FROM costs WHERE key=?1",
                [&first.key],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            10
        );
        for n in 0..CAP + 2 {
            let mut s = first.clone();
            s.key = format!("key-{n}");
            s.receipt = format!("receipt-{n}");
            s.observed = n as u64;
            assert!(record(&mut db, &s).unwrap());
        }
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM costs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            CAP as i64
        );
    }
    #[test]
    fn tails_continue_pages_but_lost_partial_or_fault_is_unknown() {
        let (worker, query) = query();
        let tails = Arc::new(Tails::default());
        let attempt = crate::ids::AttemptId::generate();
        assert!(tails
            .begin(&attempt, &worker, query.clone(), true)
            .unwrap()
            .finish(true, false)
            .is_none());
        let complete = tails
            .begin(&attempt, &worker, query.clone(), false)
            .unwrap()
            .finish(true, true)
            .unwrap();
        assert!(complete.receipt.contains(attempt.as_str()));
        assert!(tails
            .begin(&attempt, &worker, query.clone(), false)
            .is_none());
        let attempt = crate::ids::AttemptId::generate();
        drop(tails.begin(&attempt, &worker, query.clone(), true).unwrap());
        assert!(tails
            .begin(&attempt, &worker, query.clone(), true)
            .is_none());
        let attempt = crate::ids::AttemptId::generate();
        assert!(tails
            .begin(&attempt, &worker, query.clone(), false)
            .is_none());
        assert!(tails
            .begin(&attempt, &worker, query.clone(), true)
            .is_none());
        let attempt = crate::ids::AttemptId::generate();
        assert!(tails
            .begin(&attempt, &worker, query.clone(), true)
            .unwrap()
            .finish(false, true)
            .is_none());
        assert!(tails.begin(&attempt, &worker, query, true).is_none());
    }
    #[test]
    fn unavailable_writer_does_not_publish_measurement() {
        let dir = private_test_root();
        let (worker, query) = query();
        let model =
            MeasuredCosts::open(dir.path(), crate::results::CollectConfig::default()).unwrap();
        model.drain();
        model.record(sample(&worker, &query, Phase::Transfer, "closed"));
        assert_eq!(
            model
                .lookup_at(&worker, &query, platform::monotonic_ms())
                .ms,
            [None; 3]
        );
    }
}

#[cfg(test)]
mod binding_tests {
    use super::*;
    #[test]
    fn output_setup_and_optional_frozen_contract_bind_tail_without_inventing_setup_zero() {
        let base = serde_json::json!({"manifest_digest":"exact-manifest","commands":[{"argv":["run"],"env":{}}]});
        let query = CostQuery::task(&base, Some("source"), None);
        for (field, value) in [
            ("outputs", serde_json::json!([{"paths":["other/**"]}])),
            ("setup", serde_json::json!({"template":"other"})),
            (
                "toolchains",
                serde_json::json!({"version":1,"digest":"other"}),
            ),
        ] {
            let mut changed = base.clone();
            changed[field] = value;
            let other = CostQuery::task(&changed, Some("source"), None);
            assert_eq!(query.transfer_binding, other.transfer_binding);
            assert_ne!(query.collection_binding, other.collection_binding);
        }
        let unknown = CostQuery::task(&base, None, None);
        assert!(unknown.transfer_binding.is_none() && unknown.collection_binding.is_none());
    }
}

#[cfg(test)]
mod evidence_tests {
    use super::*;
    #[test]
    fn eviction_watermark_rejects_late_replay_and_long_phase_and_resets_previous_boot() {
        let dir = private_test_root();
        let model = MeasuredCosts::open(dir.path(), Default::default()).unwrap();
        model.drain();
        let mut db = Connection::open(dir.path().join("costs-v1.sqlite")).unwrap();
        let boot = platform::boot_id().unwrap();
        let sample = Sample {
            key: "latest".into(),
            receipt: "actual".into(),
            boot: boot.clone(),
            observed: 10,
            duration: 1,
        };
        db.execute(
            "INSERT INTO cost_eviction(id,boot,cutoff) VALUES(1,?1,10)",
            [&boot],
        )
        .unwrap();
        assert!(!record(&mut db, &sample).unwrap());
        let mut long = sample.clone();
        long.key = "late-long-phase".into();
        long.observed = 9;
        assert!(!record(&mut db, &long).unwrap());
        db.execute(
            "UPDATE cost_eviction SET boot='previous-boot',cutoff=99999 WHERE id=1",
            [],
        )
        .unwrap();
        assert!(record(&mut db, &sample).unwrap());
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM cost_eviction", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        let mut old = sample;
        old.boot = "previous-boot".into();
        old.key = "old-late".into();
        assert!(!record(&mut db, &old).unwrap());
    }
    #[test]
    fn evidence_reports_one_original_sample_and_truthful_unknown_reasons_without_writes() {
        use crate::scheduler::CostModel;
        let dir = private_test_root();
        let model = MeasuredCosts::open(dir.path(), Default::default()).unwrap();
        let worker = WorkerId::parse("worker").unwrap();
        let query = CostQuery::task(
            &serde_json::json!({"manifest_digest":"digest","commands":[]}),
            Some("source"),
            Some(Transport {
                worker_boot: WorkerBootId::parse("boot").unwrap(),
                agent_epoch: AgentEpoch(1),
                policy: "opaque-policy".into(),
            }),
        );
        let now = platform::monotonic_ms();
        let sample = Sample::verified(
            &worker,
            &query,
            Phase::Transfer,
            "receipt".into(),
            now.saturating_sub(1),
            1,
        )
        .unwrap();
        let observed = sample.observed;
        model.record(sample);
        model.drain();
        let evidence = model.evidence_at(&worker, &query, observed + 2);
        assert_eq!(evidence.transfer.kind, EvidenceKind::Measured);
        assert_eq!(evidence.transfer.sample_count, Some(1));
        assert_eq!(evidence.transfer.observed_monotonic_ms, Some(observed));
        assert_eq!(evidence.transfer.remaining_ttl_ms, Some(TTL_MS - 2));
        assert!(evidence
            .transfer
            .provenance
            .unwrap()
            .transport_policy_digest
            .starts_with("sha256:"));
        assert_eq!(evidence.setup.kind, EvidenceKind::Unknown);
        assert_eq!(
            model
                .evidence_at(&worker, &query, observed + TTL_MS + 1)
                .transfer
                .kind,
            EvidenceKind::Unknown
        );
        assert_eq!(
            model
                .evidence(&worker, &CostQuery::default())
                .unwrap()
                .transfer
                .kind,
            EvidenceKind::Unknown
        );
        let mut warm = query.clone();
        warm.ready = true;
        let warm = model.evidence_at(&worker, &warm, observed + 2);
        assert_eq!(warm.transfer.kind, EvidenceKind::VerifiedReady);
        assert_eq!(warm.transfer.sample_count, Some(0));
    }
}

#[cfg(test)]
mod storage_boundary_tests {
    use super::*;
    fn root() -> tempfile::TempDir {
        private_test_root()
    }
    #[test]
    fn symlinked_ancestor_cannot_create_outside_statistics() {
        let owned = root();
        let outside = root();
        std::os::unix::fs::symlink(outside.path(), owned.path().join("parent-link")).unwrap();
        assert!(
            MeasuredCosts::open(&owned.path().join("parent-link/stats"), Default::default())
                .is_err()
        );
        assert!(!outside.path().join("stats").exists());
        assert!(!outside.path().join("costs-v1.sqlite").exists());
    }
    #[test]
    fn tied_observation_cap_retains_same_keys_live_durable_and_after_restart() {
        let dir = root();
        let initial = MeasuredCosts::open(dir.path(), Default::default()).unwrap();
        initial.drain();
        drop(initial);
        let mut db = Connection::open(dir.path().join("costs-v1.sqlite")).unwrap();
        let boot = platform::boot_id().unwrap();
        let observed = platform::monotonic_ms().saturating_sub(1);
        {
            let tx = db.transaction().unwrap();
            for n in 0..CAP {
                tx.execute("INSERT INTO costs(version,key,receipt,boot,observed,duration) VALUES(1,?1,?2,?3,?4,1)",params![format!("key-{n:05}"),format!("receipt-{n}"),boot,observed as i64]).unwrap();
            }
            tx.commit().unwrap();
        }
        let live = MeasuredCosts::open(dir.path(), Default::default()).unwrap();
        live.record(Sample {
            key: "a-new-low-key".into(),
            receipt: "new".into(),
            boot,
            observed,
            duration: 1,
        });
        live.drain();
        let live_keys: Vec<_> = live.cache.read().unwrap().keys().cloned().collect();
        let durable_keys: Vec<String> = db
            .prepare("SELECT key FROM costs ORDER BY key")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(live_keys.len(), CAP);
        assert_eq!(live_keys, durable_keys);
        drop(live);
        let restarted = MeasuredCosts::open(dir.path(), Default::default()).unwrap();
        let restarted_keys: Vec<_> = restarted.cache.read().unwrap().keys().cloned().collect();
        assert_eq!(durable_keys, restarted_keys);
    }
}
