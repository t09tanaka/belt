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
                                let error = metadata.collection_closed_at_ms.map(|_| {
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
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel = Cancel(cancelled.clone());
    let active: ActiveWork = Arc::new(std::sync::Mutex::new(None));
    let outcome = tokio::select! {
        result=async {
            let dir=args.dir.as_ref().ok_or_else(||ApiError::new(ErrorCode::InvalidInput,"--dir is required"))?;
            let target=Target::parse(&args.id)?;
            let attempt=args.attempt.as_deref().map(AttemptId::parse).transpose().map_err(|e|ApiError::new(ErrorCode::InvalidInput,e.to_string()))?;
            if attempt.is_some() && matches!(target,Target::Run(_)){return Err(ApiError::new(ErrorCode::InvalidInput,"--attempt requires a task identifier"));}
            let env=std::env::vars_os().filter_map(|(k,v)|Some((k.into_string().ok()?,v.into_string().ok()?))).collect();
            let spec=super::context::runtime_spec(&env)?;
            let root=spec.state_dir.join("results");
            let reader=Reader::open(spec).await.map_err(query_error)?;
            let destination=Arc::new(super::output_export::destination(dir)?);
            Exporter{reader,root,destination,target,attempt,cancelled:cancelled.clone(),active:active.clone()}.export(json,&mut out,&mut err).await
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
    super::logs::emit_final(
        record,
        code,
        detached,
        json,
        &mut out,
        &mut err,
        Some(&mut interrupt),
    )
    .await
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
            let dir = tempfile::tempdir_in("/private/tmp").unwrap();
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
                &[],
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
                .record_artifact(
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
}
