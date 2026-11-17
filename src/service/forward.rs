//! Chunked immutable source delivery on an independent multiplexed worker link.
use crate::ids::*;
use crate::protocol::{transfer::put_blob_reader_as, Client};
use crate::source::{store::UploadFile, SourceStore};
use crate::wire::*;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions, TryLockError};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, OwnedMutexGuard};

// This lease outlives an interrupted request and every blocking operation it started.
// The file lock also excludes a restarted owner's exporter/cleanup while old IO ends.
pub(super) struct Lease {
    _file: LockedTransfer,
    _source: crate::source::retention::Guard,
    measurable: std::sync::atomic::AtomicBool,
    started: u64,
    // Local ownership is the drain barrier: release it after both file locks.
    _local: OwnedMutexGuard<()>,
}
struct LockedTransfer(File);
impl Drop for LockedTransfer {
    fn drop(&mut self) {
        // A fork/dup can retain the open description after our File is closed.
        // Only the final Arc<Lease> releases this ownership, before local handoff.
        let _ = self.0.unlock();
    }
}
// Retain the existing transfer lease even if cancellation drops the upload
// future while one bounded blocking read is still finishing.
struct OwnedUpload {
    file: crate::source::store::SourceReader,
    _lease: Arc<Lease>,
    read: u64,
    size: u64,
}
impl std::io::Read for OwnedUpload {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let count = std::io::Read::read(&mut self.file, buf)?;
        self.read = self.read.saturating_add(count as u64);
        Ok(count)
    }
}
impl Drop for OwnedUpload {
    fn drop(&mut self) {
        if self.read != self.size {
            self._lease
                .measurable
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }
}
impl std::io::Seek for OwnedUpload {
    fn seek(&mut self, from: std::io::SeekFrom) -> std::io::Result<u64> {
        let offset = std::io::Seek::seek(&mut self.file, from)?;
        if !matches!(from, std::io::SeekFrom::End(0)) && offset != self.read {
            self._lease
                .measurable
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(offset)
    }
}
async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|value| *value).await;
}
fn check_stop(stop: &watch::Receiver<bool>) -> anyhow::Result<()> {
    anyhow::ensure!(!*stop.borrow(), "resident is stopping");
    Ok(())
}

const TIMEOUT: Duration = Duration::from_secs(5);

/// One manifest as the wire carries it (compact JSON) is at most what a
/// worker accepts by default (`BlobLimits::default().max_manifest_bytes`,
/// spec chapter 11): a larger one is refused here, before anything of the
/// source is sent.
fn max_manifest_bytes() -> u64 {
    crate::worker::blob_policy::BlobLimits::default().max_manifest_bytes
}

/// Counts and hashes what passes, and refuses the byte that exceeds `limit`.
struct Capped<W> {
    inner: W,
    hasher: sha2::Sha256,
    written: u64,
    limit: u64,
}
impl<W: std::io::Write> std::io::Write for Capped<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.limit - self.written < bytes.len() as u64 {
            return Err(std::io::Error::other(format!(
                "the manifest is larger than {} bytes",
                self.limit
            )));
        }
        let count = self.inner.write(bytes)?;
        sha2::Digest::update(&mut self.hasher, &bytes[..count]);
        self.written += count as u64;
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// A spooled manifest: an unnamed private file with its digest and size.
struct Spooled {
    file: File,
    digest: Digest,
    size: u64,
}

/// Write `value` as compact JSON to an unnamed temporary file in `dir`,
/// hashing it on the way. The JSON is never held in memory: the serializer
/// writes through one 64 KiB buffer, and the upload reads the file back in
/// chunks of that size. `dir` is the private scratch directory of the source
/// store, on the disk of the state, so the spool is not memory either (the
/// system temporary directory may be). Synchronous IO; callers run it off
/// the executor threads.
fn spool_json<T: serde::Serialize>(
    value: &T,
    dir: &std::path::Path,
    limit: u64,
) -> anyhow::Result<Spooled> {
    use std::io::{Seek, Write};
    let mut out = std::io::BufWriter::with_capacity(
        64 * 1024,
        Capped {
            inner: tempfile::tempfile_in(dir)?,
            hasher: <sha2::Sha256 as sha2::Digest>::new(),
            written: 0,
            limit,
        },
    );
    serde_json::to_writer(&mut out, value)?;
    out.flush()?;
    let Capped {
        inner: mut file,
        hasher,
        written: size,
        ..
    } = out.into_inner().map_err(|e| e.into_error())?;
    let hash: String = sha2::Digest::finalize(hasher)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    file.rewind()?;
    Ok(Spooled {
        file,
        digest: Digest::parse(&format!("sha256:{hash}"))?,
        size,
    })
}

/// Keeps the transfer lease through the last blocking read of a spooled
/// manifest, as [`OwnedUpload`] does for a file of the store.
struct SpooledUpload {
    file: File,
    _lease: Arc<Lease>,
}
impl std::io::Read for SpooledUpload {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        std::io::Read::read(&mut self.file, buf)
    }
}
impl std::io::Seek for SpooledUpload {
    fn seek(&mut self, from: std::io::SeekFrom) -> std::io::Result<u64> {
        std::io::Seek::seek(&mut self.file, from)
    }
}

#[allow(clippy::too_many_arguments)]
async fn put_reader<R: std::io::Read + std::io::Seek + Send + 'static>(
    client: &Client,
    epoch: CoordinatorEpoch,
    source: &SourceId,
    kind: SourceBlobKind,
    reader: R,
    digest: Digest,
    size: u64,
    stop: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    check_stop(stop)?;
    let answer = tokio::select! {
        biased;
        _=stopped(stop)=>anyhow::bail!("resident is stopping"),
        answer=put_blob_reader_as(client,epoch,source,kind,reader,digest,size,0,64*1024,TIMEOUT)=>answer?,
    };
    anyhow::ensure!(answer.complete, "source transfer incomplete");
    Ok(())
}
async fn put_file(
    client: &Client,
    epoch: CoordinatorEpoch,
    source: &SourceId,
    kind: SourceBlobKind,
    upload: UploadFile,
    stop: &mut watch::Receiver<bool>,
    lease: Arc<Lease>,
) -> anyhow::Result<()> {
    let reader = OwnedUpload {
        file: upload.file,
        _lease: lease,
        read: 0,
        size: upload.size,
    };
    put_reader(
        client,
        epoch,
        source,
        kind,
        reader,
        upload.digest,
        upload.size,
        stop,
    )
    .await
}
pub(super) async fn verify(
    client: &Client,
    source: &SourceId,
    stop: &mut watch::Receiver<bool>,
) -> anyhow::Result<BlobVerifyResponse> {
    check_stop(stop)?;
    let call = client.call(
        Request::BlobVerify(BlobVerifyRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            source_id: source.clone(),
            blobs: vec![],
        }),
        TIMEOUT,
    );
    let answer = tokio::select! {
        biased;
        _=stopped(stop)=>anyhow::bail!("resident is stopping"),
        answer=call=>answer?,
    };
    match answer {
        Response::BlobVerify(answer) => Ok(answer),
        _ => anyhow::bail!("unexpected source verification answer"),
    }
}
/// Shared by source forwarding and retry. Future source/Vault GC must hold this
/// same cross-process lease; blocking IO retains an Arc until it has drained.
pub(super) async fn lease(
    store: &SourceStore,
    stop: &mut watch::Receiver<bool>,
    ownership: OwnedMutexGuard<()>,
) -> anyhow::Result<Arc<Lease>> {
    check_stop(stop)?;
    let source = store.mutation_guard()?;
    lease_guarded(store, stop, ownership, source).await
}

/// Foreground retry input ownership. Blocking Source SH acquisition happens
/// outside admission/the coordinator actor and never on an async runtime worker.
/// On stop, detached acquisition keeps local ownership until it actually drains.
pub(super) async fn foreground_lease(
    store: &SourceStore,
    stop: &mut watch::Receiver<bool>,
    ownership: OwnedMutexGuard<()>,
) -> anyhow::Result<Arc<Lease>> {
    check_stop(stop)?;
    let sources = store.clone();
    let acquisition = tokio::task::spawn_blocking(move || {
        sources.foreground_guard().map(|source| (source, ownership))
    });
    let (source, ownership) = tokio::select! {
        biased;
        _=stopped(stop)=>anyhow::bail!("resident is stopping"),
        result=acquisition=>result??,
    };
    lease_guarded(store, stop, ownership, source).await
}

async fn lease_guarded(
    store: &SourceStore,
    stop: &mut watch::Receiver<bool>,
    ownership: OwnedMutexGuard<()>,
    source: crate::source::retention::Guard,
) -> anyhow::Result<Arc<Lease>> {
    // Preserve source-before-local teardown on error/cancellation while waiting
    // for transfer.lock too; tuple fields drop in declaration order.
    let input = (source, ownership);
    check_stop(stop)?;
    let path = store.root().join("transfer.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    anyhow::ensure!(
        meta.is_file()
            && meta.nlink() == 1
            && meta.uid() == rustix::process::geteuid().as_raw()
            && meta.mode() & 0o077 == 0,
        "unsafe source transfer lock"
    );
    loop {
        check_stop(stop)?;
        match file.try_lock() {
            Ok(()) => break,
            Err(TryLockError::WouldBlock) => {
                tokio::select! {biased; _=stopped(stop)=>anyhow::bail!("resident is stopping"), _=tokio::time::sleep(Duration::from_millis(25))=>{}}
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(Arc::new(Lease {
        _local: input.1,
        _file: LockedTransfer(file),
        _source: input.0,
        measurable: std::sync::atomic::AtomicBool::new(true),
        started: crate::platform::monotonic_ms(),
    }))
}

/// Returns the verified phase origin after both local and file ownership leases.
pub(super) async fn source_timed(
    client: &Client,
    epoch: CoordinatorEpoch,
    store: SourceStore,
    source: &SourceId,
    env: BTreeMap<String, String>,
    mut stop: watch::Receiver<bool>,
    ownership: OwnedMutexGuard<()>,
) -> anyhow::Result<Option<u64>> {
    let lease = lease(&store, &mut stop, ownership).await?;
    let started = lease.started;
    let outcome = source_inner(client, epoch, &store, source, env, &mut stop, &lease).await;
    // Do not abort the worker on request cancellation: an exporter already on a
    // blocking thread must finish before cleanup. Neither job owns CoordinatorStore.
    let trace = lease.clone();
    let clean = store.clone();
    let lease = lease.clone();
    if let Some(id) = crate::source::classify::ci_id_of(source) {
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            clean.remove_bundles(&id)
        })
        .await??;
    }
    outcome.map(|_| {
        trace
            .measurable
            .load(std::sync::atomic::Ordering::Relaxed)
            .then_some(started)
    })
}
async fn source_inner(
    client: &Client,
    epoch: CoordinatorEpoch,
    store: &SourceStore,
    source: &SourceId,
    env: BTreeMap<String, String>,
    stop: &mut watch::Receiver<bool>,
    lease: &Arc<Lease>,
) -> anyhow::Result<()> {
    let id = crate::source::classify::ci_id_of(source)
        .ok_or_else(|| anyhow::anyhow!("invalid captured source"))?;
    let read = store.clone();
    let ownership = lease.clone();
    let (manifest, closure) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let _ownership = ownership;
        let manifest = read
            .manifest(&id)?
            .ok_or_else(|| anyhow::anyhow!("input source unavailable"))?;
        let closure = read.closure(&manifest)?;
        Ok((manifest, closure))
    })
    .await??;
    check_stop(stop)?;
    if verify(client, source, stop).await?.ready {
        lease
            .measurable
            .store(false, std::sync::atomic::Ordering::Relaxed);
        return Ok(());
    }
    // The wire requires the root manifest first; remaining manifests are its closure.
    // Secrets are metadata only.
    // The JSON of a manifest goes through a spool file, 64 KiB at a time.
    let closure = Arc::new(closure);
    for index in (0..closure.len()).rev() {
        let (items, ownership) = (closure.clone(), lease.clone());
        let scratch = store.root().join("tmp");
        let spooled = tokio::task::spawn_blocking(move || {
            let _ownership = ownership;
            spool_json(&items[index], &scratch, max_manifest_bytes())
        })
        .await??;
        check_stop(stop)?;
        if spooled.size > 64 * 1024 {
            lease
                .measurable
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
        let reader = SpooledUpload {
            file: spooled.file,
            _lease: lease.clone(),
        };
        put_reader(
            client,
            epoch,
            source,
            SourceBlobKind::Manifest,
            reader,
            spooled.digest,
            spooled.size,
            stop,
        )
        .await?;
    }
    let mut hashes = BTreeMap::new();
    for item in closure.iter() {
        for entry in &item.object_manifest.entries {
            if let Some(size) = hashes.insert(entry.hash.clone(), entry.size) {
                anyhow::ensure!(size == entry.size, "source blob has conflicting sizes");
            }
        }
    }
    for (hash, size) in hashes {
        let read = store.clone();
        let ownership = lease.clone();
        let upload = tokio::task::spawn_blocking(move || {
            let _ownership = ownership;
            let upload = read.open_blob_verified(&hash)?;
            anyhow::ensure!(upload.size == size, "source blob size mismatch");
            Ok::<_, anyhow::Error>(upload)
        })
        .await??;
        check_stop(stop)?;
        put_file(
            client,
            epoch,
            source,
            SourceBlobKind::Content,
            upload,
            stop,
            lease.clone(),
        )
        .await?;
    }
    // At most the original bundle and two bounded base rebuilds.
    let outcome = async {
        for _ in 0..3 {
            let response = verify(client, source, stop).await?;
            if response.ready { return Ok(()); }
            anyhow::ensure!(response.bundle_needed, "source verification did not request a bundle");
            let have = response.have.into_iter().flat_map(|h| h.commits).collect::<Vec<_>>();
            if !have.is_empty() {lease.measurable.store(false,std::sync::atomic::Ordering::Relaxed);}
            let read = store.clone(); let manifest = manifest.clone(); let env = env.clone();
            let ownership=lease.clone(); let cancellation=stop.clone();
            let upload = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                let _ownership=ownership;
                let path = read.export_bundle(&local_ci::ExecContext::new(env), &manifest, &have)?;
                let upload=read.open_bundle_verified(&path);
                // Runtime teardown may drop the async job while this export is
                // still running. Finish cleanup under the retained file lease.
                if *cancellation.borrow() {
                    read.remove_bundles(&manifest.source_id)?;
                    anyhow::bail!("resident is stopping");
                }
                upload
            }).await??;
            match put_file(client, epoch, source, SourceBlobKind::Bundle, upload, stop, lease.clone()).await {
                Ok(()) => {},
                Err(error) if error.downcast_ref::<crate::protocol::transfer::TransferError>().is_some_and(|e| matches!(e,
                    crate::protocol::transfer::TransferError::Call(crate::protocol::CallError::Remote(api)) if matches!(api.conflict_reason, Some(crate::error::ConflictReason::BundleBaseMissing { .. })))) => continue,
                Err(error) => return Err(error),
            }
        }
        anyhow::ensure!(verify(client, source, stop).await?.ready, "source bundle rebuild limit reached");
        Ok(())
    }.await;
    outcome
}

#[cfg(test)]
mod measured_tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};
    async fn upload() -> (tempfile::TempDir, Arc<Lease>, OwnedUpload) {
        let dir = crate::test_tmp::tempdir().unwrap();
        let source = SourceStore::open(dir.path()).unwrap();
        let local = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
        let lease = Arc::new(Lease {
            _local: local,
            _file: LockedTransfer(tempfile::tempfile().unwrap()),
            _source: source.mutation_guard().unwrap(),
            measurable: std::sync::atomic::AtomicBool::new(true),
            started: crate::platform::monotonic_ms(),
        });
        let (hash, _) = source
            .object_store()
            .unwrap()
            .put_bytes(b"abcdefgh")
            .unwrap();
        let file = source.open_blob_verified(&hash).unwrap().file;
        let reader = OwnedUpload {
            file,
            _lease: lease.clone(),
            read: 0,
            size: 8,
        };
        (dir, lease, reader)
    }
    #[tokio::test]
    async fn contiguous_chunks_with_size_probes_are_complete_but_resume_is_unknown() {
        let (_dir, lease, mut reader) = upload().await;
        let mut bytes = [0; 4];
        assert_eq!(reader.seek(SeekFrom::End(0)).unwrap(), 8);
        reader.seek(SeekFrom::Start(0)).unwrap();
        reader.read_exact(&mut bytes).unwrap();
        reader.seek(SeekFrom::End(0)).unwrap();
        reader.seek(SeekFrom::Start(4)).unwrap();
        reader.read_exact(&mut bytes).unwrap();
        drop(reader);
        assert!(lease.measurable.load(std::sync::atomic::Ordering::Relaxed));
        let (_dir, lease, mut reader) = upload().await;
        reader.seek(SeekFrom::Start(4)).unwrap();
        reader.read_exact(&mut bytes).unwrap();
        drop(reader);
        assert!(!lease.measurable.load(std::sync::atomic::Ordering::Relaxed));
        let (_dir, lease, mut reader) = upload().await;
        reader.seek(SeekFrom::Start(0)).unwrap();
        reader.read_exact(&mut bytes).unwrap();
        drop(reader);
        assert!(!lease.measurable.load(std::sync::atomic::Ordering::Relaxed));
    }
}

#[cfg(test)]
mod measured_lease_tests {
    use super::*;
    #[tokio::test]
    async fn final_lease_drop_releases_inherited_transfer_description_before_local_handoff() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = SourceStore::open(dir.path()).unwrap();
        let local = Arc::new(tokio::sync::Mutex::new(()));
        let (_stop, mut stopping) = watch::channel(false);
        let owner = lease(&store, &mut stopping, local.clone().lock_owned().await)
            .await
            .unwrap();
        let inherited = owner._file.0.try_clone().unwrap();
        let independent = OpenOptions::new()
            .read(true)
            .write(true)
            .open(store.root().join("transfer.lock"))
            .unwrap();
        let retained = owner.clone();
        drop(owner);
        assert!(matches!(
            independent.try_lock(),
            Err(TryLockError::WouldBlock)
        ));
        assert!(store.retention.guard(true).is_err());
        assert!(local.try_lock().is_err());
        drop(retained);
        let _next = local.lock_owned().await;
        independent.try_lock().unwrap();
        let _exclusive = store.retention.guard(true).unwrap();
        drop(inherited);
        assert!(matches!(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(store.root().join("transfer.lock"))
                .unwrap()
                .try_lock(),
            Err(TryLockError::WouldBlock)
        ));
    }

    #[tokio::test]
    async fn phase_origin_excludes_local_and_file_ownership_waits() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = SourceStore::open(dir.path()).unwrap();
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(store.root().join("transfer.lock"))
            .unwrap();
        lock.try_lock().unwrap();
        let local = Arc::new(tokio::sync::Mutex::new(()));
        let blocked = local.clone().lock_owned().await;
        let (_keep_alive, mut stop) = watch::channel(false);
        let before = crate::platform::monotonic_ms();
        let mut job = tokio::spawn(async move {
            let ownership = local.lock_owned().await;
            lease(&store, &mut stop, ownership).await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!job.is_finished());
        drop(blocked);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!job.is_finished());
        let released = crate::platform::monotonic_ms();
        drop(lock);
        let acquired = tokio::time::timeout(Duration::from_secs(1), &mut job)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(acquired.started >= released);
        assert!(acquired.started.saturating_sub(before) >= 50);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn foreground_source_wait_stop_and_cancellation_keep_local_ownership_until_drain() {
        for cancel in [false, true] {
            let dir = crate::test_tmp::tempdir().unwrap();
            let store = SourceStore::open(dir.path()).unwrap();
            let exclusive = store.retention.guard(true).unwrap();
            let local = Arc::new(tokio::sync::Mutex::new(()));
            let ownership = local.clone().lock_owned().await;
            let (stop, mut stopping) = watch::channel(false);
            let sources = store.clone();
            let pending =
                tokio::spawn(
                    async move { foreground_lease(&sources, &mut stopping, ownership).await },
                );
            tokio::time::timeout(Duration::from_secs(1), async {
                while store.foreground_wait_count() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if cancel {
                pending.abort();
                assert!(matches!(pending.await, Err(error) if error.is_cancelled()));
            } else {
                stop.send(true).unwrap();
                assert!(tokio::time::timeout(Duration::from_secs(1), pending)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_err());
            }
            // Async cancellation is not cancellation of the owned blocking IO.
            assert!(local.try_lock().is_err());
            drop(exclusive);
            let _drained = tokio::time::timeout(Duration::from_secs(2), local.lock_owned())
                .await
                .unwrap();
            let _exclusive = store.retention.guard(true).unwrap();
        }
    }

    #[tokio::test]
    async fn foreground_transfer_wait_stop_releases_source_before_local_handoff() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = SourceStore::open(dir.path()).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(store.root().join("transfer.lock"))
            .unwrap();
        file.try_lock().unwrap();
        let local = Arc::new(tokio::sync::Mutex::new(()));
        let ownership = local.clone().lock_owned().await;
        let (stop, mut stopping) = watch::channel(false);
        let sources = store.clone();
        let pending =
            tokio::spawn(async move { foreground_lease(&sources, &mut stopping, ownership).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.retention.guard(true).is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop.send(true).unwrap();
        assert!(pending.await.unwrap().is_err());
        let _next = local.lock_owned().await;
        let _exclusive = store.retention.guard(true).unwrap();
        // Stopping the waiter must not unlock the separate transfer owner.
        assert!(matches!(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(store.root().join("transfer.lock"))
                .unwrap()
                .try_lock(),
            Err(TryLockError::WouldBlock)
        ));
    }

    #[tokio::test]
    async fn cancelled_ownership_wait_has_no_measurement_origin() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = SourceStore::open(dir.path()).unwrap();
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(store.root().join("transfer.lock"))
            .unwrap();
        lock.try_lock().unwrap();
        let ownership = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
        let (cancel, mut stop) = watch::channel(false);
        let job = tokio::spawn(async move { lease(&store, &mut stop, ownership).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!job.is_finished());
        cancel.send(true).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(1), job)
            .await
            .unwrap()
            .unwrap()
            .is_err());
    }
}

#[cfg(test)]
mod spool_tests {
    use super::*;
    use std::io::Read;

    fn value(len: usize) -> serde_json::Value {
        serde_json::json!({"entries": vec!["0123456789abcdef"; len]})
    }

    #[test]
    fn the_spool_holds_the_compact_json_with_its_digest_and_size() {
        // Larger than the 64 KiB buffer, so it reaches the file in pieces.
        let v = value(20_000);
        let want = serde_json::to_vec(&v).unwrap();
        assert!(want.len() > 3 * 64 * 1024);
        let dir = crate::test_tmp::tempdir().unwrap();
        let mut spooled = spool_json(&v, dir.path(), want.len() as u64).unwrap();
        assert_eq!(spooled.size, want.len() as u64);
        assert_eq!(spooled.digest, Digest::of_bytes(&want));
        // It is rewound: the upload reads it from the start.
        let mut got = Vec::new();
        spooled.file.read_to_end(&mut got).unwrap();
        assert_eq!(got, want);
        // It has no name: nothing is left to clean up, whatever happens.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        // Without the directory there is no spool (it is not made elsewhere).
        assert!(spool_json(&v, &dir.path().join("absent"), u64::MAX).is_err());
    }

    #[test]
    fn a_manifest_one_byte_over_the_limit_is_refused() {
        let dir = crate::test_tmp::tempdir().unwrap();
        for len in [1, 20_000] {
            let v = value(len);
            let size = serde_json::to_vec(&v).unwrap().len() as u64;
            assert_eq!(spool_json(&v, dir.path(), size).unwrap().size, size);
            let error = spool_json(&v, dir.path(), size - 1).err().unwrap();
            assert!(
                format!("{error:#}").contains(&format!("larger than {} bytes", size - 1)),
                "{error:#}"
            );
        }
        assert_eq!(max_manifest_bytes(), 67_108_864);
    }

    #[test]
    fn the_cap_counts_what_was_written_across_calls() {
        use std::io::Write;
        let mut out = Capped {
            inner: Vec::new(),
            hasher: <sha2::Sha256 as sha2::Digest>::new(),
            written: 0,
            limit: 5,
        };
        assert_eq!(out.write(b"abc").unwrap(), 3);
        assert!(out.write(b"def").is_err());
        assert_eq!(out.write(b"de").unwrap(), 2);
        assert!(out.write(b"f").is_err());
        assert_eq!((out.written, out.inner.as_slice()), (5, &b"abcde"[..]));
    }
}
