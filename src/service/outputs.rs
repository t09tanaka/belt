//! Read-only, offline-capable receipt export. Run verdict and export verdict are separate.
use super::completion::Target;
use super::query::{Query, Reader, Response};
use crate::error::{ApiError, ErrorCode};
use crate::ids::{AttemptId, TaskId};
use crate::results::store::ResultStore;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

fn internal(e: impl ToString) -> ApiError {
    ApiError::new(ErrorCode::Internal, e.to_string())
}
fn query_error(e: super::query::Error) -> ApiError {
    ApiError::new(e.code, e.message)
}
#[derive(Default, Debug, Serialize)]
pub struct Summary {
    pub tasks: u64,
    pub exported: u64,
    pub failed: u64,
    pub missing: u64,
    pub refused: u64,
    pub unreceipted: u64,
    pub pending_tasks: u64,
    pub collection_failed_tasks: u64,
    pub no_work: bool,
}
type ActiveWork = Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), ApiError>>>>>;
struct Exporter {
    active: ActiveWork,
    reader: Reader,
    root: PathBuf,
    destination: Arc<std::fs::File>,
    target: Target,
    attempt: Option<AttemptId>,
    cancelled: Arc<AtomicBool>,
}
struct Cancel(Arc<AtomicBool>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}
impl Exporter {
    async fn export(
        &self,
        json: bool,
        out: &mut (dyn AsyncWrite + Unpin),
        err: &mut (dyn AsyncWrite + Unpin),
    ) -> Result<(Summary, Option<ErrorCode>), ApiError> {
        let observation = match self
            .reader
            .query(Query::Completion {
                target: self.target.clone(),
            })
            .await
            .map_err(query_error)?
        {
            Response::Completion { snapshot } => snapshot,
            _ => return Err(internal("unexpected target observation")),
        };
        if !observation.accepted {
            return Err(ApiError::new(ErrorCode::NotFound, "target not found"));
        }
        emit(
            json,
            out,
            err,
            serde_json::json!({"kind":"target_observation","snapshot":observation}),
        )
        .await?;
        let mut summary = Summary::default();
        let mut failure = None;
        let mut after = None;
        loop {
            let (tasks, next) = match &self.target {
                Target::Run(run_id) => match self
                    .reader
                    .query(Query::Tasks {
                        run_id: run_id.clone(),
                        limit: 32,
                        after,
                    })
                    .await
                    .map_err(query_error)?
                {
                    Response::Tasks { page } => (page.items, page.next_cursor),
                    _ => return Err(internal("unexpected task page")),
                },
                Target::Task(task_id) => match self
                    .reader
                    .query(Query::Task {
                        task_id: task_id.clone(),
                    })
                    .await
                    .map_err(query_error)?
                {
                    Response::Task { task: Some(task) } => (vec![task], None),
                    Response::Task { task: None } => {
                        return Err(ApiError::new(ErrorCode::NotFound, "task not found"))
                    }
                    _ => return Err(internal("unexpected task")),
                },
            };
            for task in tasks {
                summary.tasks += 1;
                let Some(attempt_id) = self
                    .attempt
                    .clone()
                    .or_else(|| task.current_attempt.map(|a| a.attempt_id))
                else {
                    if task.cancel_requested_at_ms.is_none() {
                        summary.pending_tasks += 1;
                    }
                    continue;
                };
                let metadata = match self
                    .reader
                    .query(Query::LogMetadata {
                        task_id: task.task_id.clone(),
                        attempt_id: attempt_id.clone(),
                    })
                    .await
                    .map_err(query_error)?
                {
                    Response::LogMetadata { metadata } => metadata,
                    _ => return Err(internal("unexpected collection metadata")),
                };
                if metadata.collection_closed_at_ms.is_none() {
                    summary.pending_tasks += 1;
                }
                if metadata
                    .collection_verdict
                    .as_deref()
                    .is_some_and(|v| v != "complete")
                {
                    summary.collection_failed_tasks += 1;
                    failure = Some(crate::error::result_read_precedence(
                        failure,
                        ErrorCode::CollectionFailed,
                    ));
                }
                let mut cursor = None;
                loop {
                    let page = match self
                        .reader
                        .query(Query::OutputMetadata {
                            task_id: task.task_id.clone(),
                            attempt_id: attempt_id.clone(),
                            after: cursor,
                            limit: 32,
                        })
                        .await
                        .map_err(query_error)?
                    {
                        Response::OutputMetadata { page } => page,
                        _ => return Err(internal("unexpected output page")),
                    };
                    for entry in page.items {
                        let artifact = match entry {
                            crate::storage::query::Output::Artifact { record } => record,
                            crate::storage::query::Output::Pending { name } => {
                                summary.unreceipted += 1;
                                let collection_failed = metadata
                                    .collection_verdict
                                    .as_deref()
                                    .is_some_and(|v| v != "complete");
                                let error = metadata
                                    .collection_closed_at_ms
                                    .filter(|_| !collection_failed)
                                    .map(|_| {
                                        internal("closed collection has an unreceipted output")
                                    });
                                if let Some(e) = &error {
                                    summary.failed += 1;
                                    failure =
                                        Some(crate::error::result_read_precedence(failure, e.code));
                                }
                                emit(json,out,err,serde_json::json!({"kind":"output_status","task_id":task.task_id,"attempt_id":attempt_id,"name":name,"availability":"pending","error":error})).await?;
                                continue;
                            }
                            crate::storage::query::Output::NotStored {
                                name,
                                reason,
                                if_missing,
                            } => {
                                match reason {
                                    crate::wire::NotStoredReason::Missing => summary.missing += 1,
                                    crate::wire::NotStoredReason::Refused => summary.refused += 1,
                                }
                                emit(json,out,err,serde_json::json!({"kind":"output_status","task_id":task.task_id,"attempt_id":attempt_id,"name":name,"availability":"not_stored","reason":reason,"if_missing":if_missing})).await?;
                                continue;
                            }
                        };
                        if !crate::outputs::valid_path(&artifact.name)
                            || crate::supervisor::spool_plan::relative_path(&artifact.name)
                                .is_none()
                        {
                            return Err(ApiError::new(
                                ErrorCode::InvalidInput,
                                "unsafe artifact name",
                            ));
                        }
                        let root = self.root.clone();
                        let attempt = attempt_id.clone();
                        let destination = self.destination.clone();
                        let prefix = format!("{}/{}/{}", task.task_id, attempt_id, artifact.name);
                        let cancelled = self.cancelled.clone();
                        let (send, mut receive) = tokio::sync::mpsc::channel(1);
                        let worker =
                            tokio::task::spawn_blocking(move || -> Result<(), ApiError> {
                                if cancelled.load(Ordering::Relaxed) {
                                    return Ok(());
                                }
                                let reader =
                                    ResultStore::open_read_only(&root).map_err(internal)?;
                                let source = reader
                                    .open_artifact(&attempt, &artifact.digest, artifact.size)
                                    .map_err(internal)?;
                                let mut bundle = super::output_export::Bundle::verify_cancellable(
                                    source, &artifact, &cancelled,
                                )?;
                                bundle.publish(
                                    &destination,
                                    &prefix,
                                    &cancelled,
                                    |path, result| {
                                        if cancelled.load(Ordering::Relaxed) {
                                            return false;
                                        }
                                        send.blocking_send((path.to_owned(), result)).is_ok()
                                    },
                                );
                                Ok(())
                            });
                        *self.active.lock().map_err(internal)? = Some(worker);
                        while let Some((path, result)) = receive.recv().await {
                            match &result {
                                Ok(()) => summary.exported += 1,
                                Err(e) => {
                                    summary.failed += 1;
                                    failure =
                                        Some(crate::error::result_read_precedence(failure, e.code));
                                }
                            }
                            emit(json,out,err,serde_json::json!({"kind":"output","task_id":task.task_id,"attempt_id":attempt_id,"path":path,"error":result.err()})).await?;
                        }
                        let worker = self
                            .active
                            .lock()
                            .map_err(internal)?
                            .take()
                            .ok_or_else(|| internal("missing export worker"))?;
                        if let Err(error) = worker.await.map_err(internal)? {
                            summary.failed += 1;
                            failure =
                                Some(crate::error::result_read_precedence(failure, error.code));
                            emit(json,out,err,serde_json::json!({"kind":"output_error","task_id":task.task_id,"attempt_id":attempt_id,"error":error})).await?;
                        }
                    }
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
            after = next
                .map(|s| TaskId::parse(&s))
                .transpose()
                .map_err(internal)?;
            if after.is_none() {
                break;
            }
        }
        summary.no_work = summary.tasks == 0;
        Ok((summary, failure))
    }
}
async fn emit<'a>(
    json: bool,
    out: &'a mut (dyn AsyncWrite + Unpin),
    err: &'a mut (dyn AsyncWrite + Unpin),
    data: serde_json::Value,
) -> Result<(), ApiError> {
    let successful_file = data["kind"] == "output" && data["error"].is_null();
    let writer = if json || successful_file { out } else { err };
    let bytes = if json {
        format!(
            "{}\n",
            serde_json::json!({"schema_version":1,"ok":data["error"].is_null(),"data":data})
        )
    } else if successful_file {
        format!("{}\n", data["path"].as_str().unwrap_or_default())
    } else {
        format!("{data}\n")
    };
    writer.write_all(bytes.as_bytes()).await.map_err(internal)?;
    writer.flush().await.map_err(internal)
}
pub fn command(args: &crate::cli::OutputsArgs, json: bool) -> i32 {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {e}");
            return ErrorCode::Internal.exit_code();
        }
    };
    let code = runtime.block_on(command_async(args, json));
    runtime.shutdown_background();
    code
}
async fn command_async(args: &crate::cli::OutputsArgs, json: bool) -> i32 {
    let mut out = tokio::io::stdout();
    let mut err = tokio::io::stderr();
    let mut interrupt =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("error: {e}");
                return ErrorCode::Internal.exit_code();
            }
        };
    invocation(args, json, &mut out, &mut err, &mut interrupt, || {
        let env = std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect();
        super::context::runtime_spec(&env)
    })
    .await
}

async fn invocation(
    args: &crate::cli::OutputsArgs,
    json: bool,
    out: &mut (dyn AsyncWrite + Unpin),
    err: &mut (dyn AsyncWrite + Unpin),
    interrupt: &mut tokio::signal::unix::Signal,
    spec: impl FnOnce() -> Result<super::bootstrap::Spec, ApiError>,
) -> i32 {
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel = Cancel(cancelled.clone());
    let active: ActiveWork = Arc::new(std::sync::Mutex::new(None));
    // Keep the shared run lock through worker drain and final write/flush.
    let mut invocation_pin = None;
    let outcome = tokio::select! {
        result=async {
            let dir=args.dir.as_ref().ok_or_else(||ApiError::new(ErrorCode::InvalidInput,"--dir is required"))?;
            if dir.as_os_str().is_empty(){return Err(ApiError::new(ErrorCode::InvalidInput,"--dir must not be empty"));}
            let target=Target::parse(&args.id)?;
            let attempt=args.attempt.as_deref().map(AttemptId::parse).transpose().map_err(|e|ApiError::new(ErrorCode::InvalidInput,e.to_string()))?;
            if attempt.is_some() && matches!(target,Target::Run(_)){return Err(ApiError::new(ErrorCode::InvalidInput,"--attempt requires a task identifier"));}
            let spec=spec()?;
            let root=spec.state_dir.join("results");
            let store=tokio::task::spawn_blocking(move || super::bootstrap::read_only_store(&spec))
                .await.map_err(internal)?.map_err(|e|ApiError::new(ErrorCode::InvalidInput,e.to_string()))?;
            invocation_pin=Some(store.result_read_pin(&target,attempt.as_ref()).map_err(|e|ApiError::new(e.code(),e.to_string()))?);
            let reader=Reader::Offline(Arc::new(store));
            // Create nothing on disk for a target that does not exist.
            match reader.query(Query::Completion{target:target.clone()}).await.map_err(query_error)? {
                Response::Completion{snapshot} if snapshot.accepted=>{}
                Response::Completion{..}=>return Err(ApiError::new(ErrorCode::NotFound,"target not found")),
                _=>return Err(internal("unexpected target observation")),
            }
            let destination=Arc::new(super::output_export::destination(dir)?);
            Exporter{reader,root,destination,target,attempt,cancelled:cancelled.clone(),active:active.clone()}.export(json,out,err).await
        }=>Some(result),
        _=interrupt.recv()=>None,
    };
    cancelled.store(true, Ordering::Relaxed);
    // Drain the invocation's only owned blocking job after cooperative detach.
    // It checks cancellation at each 64 KiB boundary and cleans its own temp.
    let worker = active.lock().ok().and_then(|mut active| active.take());
    if let Some(worker) = worker {
        let _ = worker.await;
    }
    let (record, code, detached) = match outcome {
        None => (
            serde_json::json!({"schema_version":1,"ok":false,"data":{"kind":"final","detached":true,"exit_code":130}}),
            130,
            true,
        ),
        Some(Ok((summary, error))) => {
            let code = error.map_or(0, ErrorCode::exit_code);
            (
                serde_json::json!({"schema_version":1,"ok":code==0,"data":{"kind":"final","summary":summary,"error_code":error,"exit_code":code}}),
                code,
                false,
            )
        }
        Some(Err(error)) => {
            let code = error.code.exit_code();
            (
                serde_json::json!({"schema_version":1,"ok":false,"error":error,"data":{"kind":"final","observation_complete":false,"exit_code":code}}),
                code,
                false,
            )
        }
    };
    let code =
        super::logs::emit_final(record, code, detached, json, out, err, Some(interrupt)).await;
    drop(invocation_pin);
    code
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::*;
    use crate::state::{AttemptState, Outcome, Termination};
    use crate::storage::{ArtifactReceipt, CoordinatorStore, NewAttempt};
    use crate::wire::{ArtifactEncoding, ArtifactRecord};
    use clap::Parser;
    struct Fixture {
        dir: tempfile::TempDir,
        db: Arc<CoordinatorStore>,
        run: RunId,
        task: TaskId,
        attempt: AttemptId,
        artifact: ArtifactRecord,
    }
    impl Fixture {
        fn new() -> Self {
            let dir = crate::test_tmp::tempdir().unwrap();
            let db = Arc::new(CoordinatorStore::open(dir.path(), 1).unwrap());
            let run = RunId::generate();
            let task = TaskId::generate();
            let attempt = AttemptId::generate();
            db.create_run(
                &run,
                None,
                None,
                &serde_json::json!({"secret":"do_not_emit"}),
                1,
            )
            .unwrap();
            db.create_task(&task, &run, &serde_json::json!({}), 1)
                .unwrap();
            db.create_attempt(
                NewAttempt {
                    attempt_id: attempt.clone(),
                    task_id: task.clone(),
                    worker_id: WorkerId::parse("worker").unwrap(),
                    payload_hash: Digest::of_bytes(b"p"),
                    permit_id: None,
                },
                2,
            )
            .unwrap();
            for state in [
                AttemptState::Preparing,
                AttemptState::Ready,
                AttemptState::Starting,
                AttemptState::Active,
                AttemptState::Finalizing,
            ] {
                db.transition_attempt(&attempt, state, 3).unwrap();
            }
            db.finalize_attempt(
                &attempt,
                Termination::new(Outcome::Failed, Some(crate::state::Exit::Code(1)), None).unwrap(),
                4,
            )
            .unwrap();
            let artifact = ArtifactRecord {
                encoding: ArtifactEncoding::Raw,
                name: "raw".into(),
                path: "PRIVATE_DECLARATION_SENTINEL".into(),
                size: 3,
                digest: Digest::of_bytes(b"raw"),
                source_id: SourceId::generate(),
                plan_digest: Digest::of_bytes(b"plan"),
                attempt_id: attempt.clone(),
                profile_fingerprint: Digest::of_bytes(b"profile"),
            };
            db.begin_collection(
                &attempt,
                &Digest::of_bytes(b"result"),
                &serde_json::json!({"artifacts":[artifact.clone()]}),
                &[("raw".into(), true)],
                &[crate::storage::ResultRef {
                    ref_id: format!("{attempt}:result"),
                    kind: crate::storage::BlobKind::Result,
                    locator: ResultStore::result_locator(&attempt),
                }],
                5,
            )
            .unwrap();
            Self {
                dir,
                db,
                run,
                task,
                attempt,
                artifact,
            }
        }
        fn receipt(&self) {
            self.db
                .record_collected_artifact(
                    &ArtifactReceipt {
                        attempt_id: self.attempt.clone(),
                        name: self.artifact.name.clone(),
                        rel_path: self.artifact.path.clone(),
                        size: self.artifact.size,
                        digest: self.artifact.digest.clone(),
                        source_id: self.artifact.source_id.clone(),
                        plan_digest: self.artifact.plan_digest.clone(),
                        profile_fingerprint: self.artifact.profile_fingerprint.to_string(),
                    },
                    &crate::storage::ResultRef {
                        ref_id: format!("{}:artifact:{}", self.attempt, self.artifact.name),
                        kind: crate::storage::BlobKind::Result,
                        locator: ResultStore::artifact_locator(
                            &self.attempt,
                            &self.artifact.digest,
                        ),
                    },
                    6,
                )
                .unwrap();
        }
        fn exporter(&self, target: Target, attempt: Option<AttemptId>) -> Exporter {
            Exporter {
                active: Arc::new(std::sync::Mutex::new(None)),
                reader: Reader::Offline(Arc::new(
                    CoordinatorStore::open_read_only(self.dir.path()).unwrap(),
                )),
                root: self.dir.path().join("results"),
                destination: Arc::new(
                    super::super::output_export::destination(&self.dir.path().join("out")).unwrap(),
                ),
                target,
                attempt,
                cancelled: Arc::new(AtomicBool::new(false)),
            }
        }
    }
    #[test]
    fn parser_outputs_attempt_and_required_dir_validation() {
        assert!(crate::cli::Cli::try_parse_from([
            "rtr",
            "outputs",
            "task",
            "--dir",
            "out",
            "--attempt",
            "a"
        ])
        .is_ok());
        let cli = crate::cli::Cli::try_parse_from(["rtr", "--json", "outputs", "bad"]).unwrap();
        assert_eq!(crate::cli::run(cli), 2);
    }
    #[tokio::test]
    async fn only_receipted_metadata_is_visible_and_missing_blob_is_error() {
        let f = Fixture::new();
        assert!(matches!(
            f.db.query_output_metadata(&f.attempt, None, 1000)
                .unwrap()
                .items
                .as_slice(),
            [crate::storage::query::Output::Pending { .. }]
        ));
        f.receipt();
        assert_eq!(
            f.db.query_output_metadata(&f.attempt, None, 1)
                .unwrap()
                .items
                .len(),
            1
        );
        let mut out = Vec::new();
        let mut err = Vec::new();
        let (s, error) = f
            .exporter(Target::Task(f.task.clone()), None)
            .export(true, &mut out, &mut err)
            .await
            .unwrap();
        assert_eq!(s.failed, 1);
        assert_eq!(error, Some(ErrorCode::Internal));
        assert!(!f.dir.path().join("results").exists());
        assert!(!String::from_utf8(out).unwrap().contains("do_not_emit"));
    }
    #[tokio::test]
    async fn closed_failed_collection_with_unreceipted_output_exits_collection_failed() {
        let f = Fixture::new();
        let attempt = f.attempt.to_string();
        f.db.db
            .write(move |tx| {
                tx.execute(
                    "UPDATE collection SET verdict='gave_up', closed_at_ms=7 WHERE attempt_id=?1",
                    [attempt],
                )?;
                Ok(())
            })
            .unwrap();
        let mut out = Vec::new();
        let mut err = Vec::new();
        let (summary, error) = f
            .exporter(Target::Task(f.task.clone()), None)
            .export(true, &mut out, &mut err)
            .await
            .unwrap();
        assert_eq!(summary.unreceipted, 1);
        assert_eq!(error, Some(ErrorCode::CollectionFailed));
    }
    #[tokio::test]
    async fn missing_target_and_empty_dir_create_no_destination() {
        use std::os::unix::fs::PermissionsExt;
        let f = Fixture::new();
        std::fs::set_permissions(f.dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let worker = f.dir.path().join("worker");
        std::fs::create_dir(&worker).unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700)).unwrap();
        let key = f.dir.path().join("key");
        crate::secrets::key::init(&key).unwrap();
        let spec = || super::super::bootstrap::Spec {
            state_dir: f.dir.path().into(),
            worker_state_dir: worker.clone(),
            worker_id: WorkerId::parse("test").unwrap(),
            cluster_key_path: key.clone(),
            executable: std::env::current_exe().unwrap(),
            env: Default::default(),
            repo_root: None,
            expected_cluster: Some(f.db.cluster_id().clone()),
            managed_service: None,
        };
        let mut signal =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
        let typo = f.dir.path().join("typo").join("results");
        let args = crate::cli::OutputsArgs {
            id: TaskId::generate().to_string(),
            attempt: None,
            dir: Some(typo.clone()),
        };
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = invocation(&args, true, &mut out, &mut err, &mut signal, || Ok(spec())).await;
        assert_eq!(code, ErrorCode::NotFound.exit_code());
        assert!(!f.dir.path().join("typo").exists());
        let args = crate::cli::OutputsArgs {
            id: f.task.to_string(),
            attempt: None,
            dir: Some(std::path::PathBuf::new()),
        };
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = invocation(&args, true, &mut out, &mut err, &mut signal, || Ok(spec())).await;
        assert_eq!(code, ErrorCode::InvalidInput.exit_code());
    }
    #[tokio::test]
    async fn pages_over_64_tasks_retry_gap_and_membership() {
        let f = Fixture::new();
        f.receipt();
        for _ in 0..70 {
            f.db.create_task(&TaskId::generate(), &f.run, &serde_json::json!({}), 7)
                .unwrap();
        }
        let task = f.task.to_string();
        f.db.db
            .write(move |tx| {
                tx.execute("UPDATE task SET min_generation=2 WHERE task_id=?1", [task])?;
                Ok(())
            })
            .unwrap();
        let mut out = Vec::new();
        let mut err = Vec::new();
        let (s, error) = f
            .exporter(Target::Run(f.run.clone()), None)
            .export(true, &mut out, &mut err)
            .await
            .unwrap();
        assert_eq!(s.tasks, 71);
        assert_eq!(s.pending_tasks, 71);
        assert_eq!(s.failed, 0);
        assert_eq!(error, None);
        let (s, error) = f
            .exporter(Target::Task(f.task.clone()), Some(f.attempt.clone()))
            .export(true, &mut out, &mut err)
            .await
            .unwrap();
        assert_eq!(s.failed, 1);
        assert_eq!(error, Some(ErrorCode::Internal));
        let foreign = TaskId::generate();
        f.db.create_task(&foreign, &f.run, &serde_json::json!({}), 8)
            .unwrap();
        let error = f
            .exporter(Target::Task(foreign), Some(f.attempt.clone()))
            .export(true, &mut out, &mut err)
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidInput);
    }
    #[tokio::test]
    async fn failed_command_does_not_fail_export_and_not_stored_is_whitelisted() {
        let f = Fixture::new();
        f.receipt();
        let files = ResultStore::open(&f.dir.path().join("results")).unwrap();
        files
            .append_part(&f.attempt, &f.artifact.digest, 0, b"raw")
            .unwrap();
        files
            .publish_artifact(&f.attempt, &f.artifact.digest, 3)
            .unwrap();
        let text=serde_json::json!({"artifacts":[f.artifact.clone()],"artifacts_not_stored":[{"name":"optional","path":"DO_NOT_EMIT_DECLARATION_PATH","if_missing":"optional","reason":"refused"}]}).to_string();
        let attempt = f.attempt.to_string();
        f.db.db
            .write(move |tx| {
                tx.execute(
                    "UPDATE collection SET result_json=?1 WHERE attempt_id=?2",
                    rusqlite::params![text, attempt],
                )?;
                Ok(())
            })
            .unwrap();
        let page = f.db.query_output_metadata(&f.attempt, None, 64).unwrap();
        let public = serde_json::to_value(&page).unwrap();
        let record = public["items"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|item| item["record"].as_object())
            .unwrap();
        assert_eq!(
            record
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from(["digest", "encoding", "name", "size"])
        );
        let encoded = public.to_string();
        for private in [
            "PRIVATE_DECLARATION_SENTINEL",
            "path",
            "source_id",
            "plan_digest",
            "profile_fingerprint",
            "attempt_id",
        ] {
            assert!(!encoded.contains(private), "{private}");
        }
        let mut out = Vec::new();
        let mut err = Vec::new();
        let (summary, error) = f
            .exporter(Target::Task(f.task.clone()), None)
            .export(true, &mut out, &mut err)
            .await
            .unwrap();
        assert_eq!(summary.exported, 1);
        assert_eq!(summary.refused, 1);
        assert_eq!(summary.failed, 0);
        assert_eq!(error, None);
        let text = String::from_utf8(out).unwrap();
        assert!(!text.contains("DO_NOT_EMIT_DECLARATION_PATH"));
        assert!(!text.contains("PRIVATE_DECLARATION_SENTINEL"));
        assert!(text.contains("COMMAND_FAILED"));
        assert!(text.contains("not_stored"));
    }
    #[test]
    fn metadata_pages_are_capped_and_mismatched_receipts_fail() {
        let f = Fixture::new();
        f.receipt();
        let mut artifacts = vec![];
        for n in 0..70 {
            let mut a = f.artifact.clone();
            a.name = format!("a{n:03}");
            a.path = a.name.clone();
            f.db.record_artifact(
                &ArtifactReceipt {
                    attempt_id: f.attempt.clone(),
                    name: a.name.clone(),
                    rel_path: a.path.clone(),
                    size: a.size,
                    digest: a.digest.clone(),
                    source_id: a.source_id.clone(),
                    plan_digest: a.plan_digest.clone(),
                    profile_fingerprint: a.profile_fingerprint.to_string(),
                },
                6,
            )
            .unwrap();
            artifacts.push(a);
        }
        let text = serde_json::json!({"artifacts":artifacts}).to_string();
        let attempt = f.attempt.to_string();
        f.db.db
            .write(move |tx| {
                tx.execute(
                    "UPDATE collection SET result_json=?1 WHERE attempt_id=?2",
                    rusqlite::params![text, attempt],
                )?;
                Ok(())
            })
            .unwrap();
        let page = f.db.query_output_metadata(&f.attempt, None, 1000).unwrap();
        assert_eq!(page.items.len(), 64);
        let page =
            f.db.query_output_metadata(&f.attempt, page.next_cursor, 1000)
                .unwrap();
        assert_eq!(page.items.len(), 6);
        assert!(page.next_cursor.is_none());
        let attempt = f.attempt.to_string();
        f.db.db
            .write(move |tx| {
                tx.execute(
                    "UPDATE artifact_receipt SET size=size+1 WHERE attempt_id=?1 AND name='a000'",
                    [attempt],
                )?;
                Ok(())
            })
            .unwrap();
        assert!(f.db.query_output_metadata(&f.attempt, None, 1).is_err());
    }
    struct Gate {
        reached: tokio::sync::Notify,
        released: AtomicBool,
        waker: std::sync::Mutex<Option<std::task::Waker>>,
    }
    struct GateWriter {
        bytes: Vec<u8>,
        kind: &'static str,
        flush: bool,
        seen: bool,
        gate: Arc<Gate>,
    }
    impl GateWriter {
        fn blocked(&self, cx: &std::task::Context<'_>) -> bool {
            if self.gate.released.load(Ordering::SeqCst) {
                return false;
            }
            *self.gate.waker.lock().unwrap() = Some(cx.waker().clone());
            self.gate.reached.notify_one();
            true
        }
    }
    impl AsyncWrite for GateWriter {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            let record: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            if record["data"]["kind"] == self.kind {
                self.seen = true;
                if !self.flush && self.blocked(cx) {
                    return std::task::Poll::Pending;
                }
            }
            self.bytes.extend_from_slice(bytes);
            std::task::Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if self.flush && self.seen && self.blocked(cx) {
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(Ok(()))
            }
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    #[tokio::test]
    async fn invocation_pin_excludes_gc_during_raw_tar_export_and_final_write_flush() {
        use std::os::unix::fs::PermissionsExt;
        for encoding in [ArtifactEncoding::Raw, ArtifactEncoding::TarV1] {
            for (kind, flush) in [("output", false), ("final", false), ("final", true)] {
                let mut f = Fixture::new();
                std::fs::set_permissions(f.dir.path(), std::fs::Permissions::from_mode(0o700))
                    .unwrap();
                let bytes = if encoding == ArtifactEncoding::Raw {
                    b"raw".to_vec()
                } else {
                    let mut builder = tar::Builder::new(Vec::new());
                    let mut header = tar::Header::new_gnu();
                    header.set_size(3);
                    header.set_mode(0o600);
                    header.set_cksum();
                    builder
                        .append_data(&mut header, "member", b"tar".as_slice())
                        .unwrap();
                    builder.into_inner().unwrap()
                };
                f.artifact.encoding = encoding;
                f.artifact.digest = Digest::of_bytes(&bytes);
                f.artifact.size = bytes.len() as u64;
                let text = serde_json::json!({"artifacts":[f.artifact.clone()]}).to_string();
                let id = f.attempt.to_string();
                f.db.db
                    .write(move |tx| {
                        tx.execute(
                            "UPDATE collection SET result_json=?1 WHERE attempt_id=?2",
                            rusqlite::params![text, id],
                        )?;
                        Ok(())
                    })
                    .unwrap();
                f.receipt();
                f.db.close_collection(
                    &f.attempt,
                    Some(&Digest::of_bytes(b"result")),
                    crate::results::plan::Verdict::Complete,
                    true,
                    8,
                )
                .unwrap();
                let files = ResultStore::open(&f.dir.path().join("results")).unwrap();
                files
                    .append_part(&f.attempt, &f.artifact.digest, 0, &bytes)
                    .unwrap();
                files
                    .publish_artifact(&f.attempt, &f.artifact.digest, f.artifact.size)
                    .unwrap();
                let worker = f.dir.path().join("worker");
                std::fs::create_dir(&worker).unwrap();
                std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700)).unwrap();
                let key = f.dir.path().join("key");
                crate::secrets::key::init(&key).unwrap();
                let spec = super::super::bootstrap::Spec {
                    state_dir: f.dir.path().into(),
                    worker_state_dir: worker,
                    worker_id: WorkerId::parse("test").unwrap(),
                    cluster_key_path: key,
                    executable: std::env::current_exe().unwrap(),
                    env: Default::default(),
                    repo_root: None,
                    expected_cluster: Some(f.db.cluster_id().clone()),
                    managed_service: None,
                };
                let args = crate::cli::OutputsArgs {
                    id: f.task.to_string(),
                    attempt: None,
                    dir: Some(f.dir.path().join("exported")),
                };
                let gate = Arc::new(Gate {
                    reached: tokio::sync::Notify::new(),
                    released: AtomicBool::new(false),
                    waker: Default::default(),
                });
                let mut out = GateWriter {
                    bytes: Vec::new(),
                    kind,
                    flush,
                    seen: false,
                    gate: gate.clone(),
                };
                let mut err = Vec::new();
                let mut signal =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                        .unwrap();
                let control = async {
                    gate.reached.notified().await;
                    assert!(
                        matches!(f.db.result_gc_guard(&f.run), Err(crate::storage::StorageError::Io(e)) if e.kind()==std::io::ErrorKind::WouldBlock)
                    );
                    gate.released.store(true, Ordering::SeqCst);
                    gate.waker.lock().unwrap().take().unwrap().wake();
                };
                let (code, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    tokio::join!(
                        invocation(&args, true, &mut out, &mut err, &mut signal, || Ok(spec)),
                        control
                    )
                })
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "{encoding:?}/{kind}/{flush}: {}",
                        String::from_utf8_lossy(&out.bytes)
                    )
                });
                assert_eq!(code, 0, "{}", String::from_utf8_lossy(&out.bytes));
                assert!(out.seen);
                drop(crate::test_tmp::gc_guard(&f.db, &f.run));
                let path = f
                    .dir
                    .path()
                    .join("exported")
                    .join(f.task.as_str())
                    .join(f.attempt.as_str())
                    .join("raw");
                assert_eq!(
                    std::fs::read(if encoding == ArtifactEncoding::Raw {
                        path
                    } else {
                        path.join("member")
                    })
                    .unwrap(),
                    if encoding == ArtifactEncoding::Raw {
                        b"raw"
                    } else {
                        b"tar"
                    }
                );
            }
        }
    }
    #[tokio::test]
    async fn deletion_claim_before_filesystem_and_completed_gc_are_unavailable() {
        let f = Fixture::new();
        f.receipt();
        let files = ResultStore::open(&f.dir.path().join("results")).unwrap();
        files.write_result(&f.attempt, b"{}").unwrap();
        files
            .append_part(&f.attempt, &f.artifact.digest, 0, b"raw")
            .unwrap();
        files
            .publish_artifact(&f.attempt, &f.artifact.digest, 3)
            .unwrap();
        f.db.close_collection(
            &f.attempt,
            Some(&Digest::of_bytes(b"result")),
            crate::results::plan::Verdict::Complete,
            true,
            8,
        )
        .unwrap();
        let guard = crate::test_tmp::gc_guard(&f.db, &f.run);
        assert!(
            f.db.claim_result_page(&guard, &f.run, "", 64, 100, 10, Some("boot".into()), 10)
                .unwrap()
                .clock_held
        );
        let page =
            f.db.claim_result_page(&guard, &f.run, "", 64, 110, 20, Some("boot".into()), 10)
                .unwrap();
        assert_eq!(page.removed, 2);
        let claims = f.db.pending_result_page(&guard, &f.run, 64).unwrap();
        assert_eq!(claims.len(), 2);
        // Crash-before-FS leaves published bytes, but removes authorization.
        drop(guard);
        let reader = CoordinatorStore::open_read_only(f.dir.path()).unwrap();
        let pin = reader
            .result_read_pin(&Target::Task(f.task.clone()), None)
            .unwrap();
        assert!(files.artifact_path(&f.attempt, &f.artifact.digest).exists());
        assert_eq!(
            reader
                .query_output_metadata(&f.attempt, None, 64)
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        let error = f
            .exporter(Target::Task(f.task.clone()), None)
            .export(true, &mut Vec::new(), &mut Vec::new())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::NotFound);
        drop(pin);
        let guard = crate::test_tmp::gc_guard(&f.db, &f.run);
        for claim in claims {
            files.remove(&claim.locator).unwrap();
            f.db.finish_result_delete(&guard, &claim).unwrap();
        }
        drop(guard);
        assert_eq!(
            CoordinatorStore::open_read_only(f.dir.path())
                .unwrap()
                .query_output_metadata(&f.attempt, None, 64)
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
    }
}
