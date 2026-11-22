//! Public log observation. All coordinator access is read-only. The invocation's
//! private SQLite cursor bounds RAM independently of task count; it is not a
//! persistent resume contract and contains no log bytes or execution inputs.
use super::completion::Target;
use super::query::{LogMetadata, Query, Reader, Response};
use crate::error::{ApiError, ErrorCode};
use crate::ids::{AttemptId, RunId, TaskId};
use crate::results::store::ResultStore;
use crate::storage::query::Task;
use crate::wire::LogStream;
use base64::Engine;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use tokio::io::{AsyncWrite, AsyncWriteExt};

fn internal(error: impl ToString) -> ApiError {
    ApiError::new(ErrorCode::Internal, error.to_string())
}
fn query_error(error: super::query::Error) -> ApiError {
    ApiError::new(error.code, error.to_string())
}

/// Lifetime encloses every cursor operation. The exclusive regular file is
/// created inside a mode-0700 TempDir before SQLite opens it (no symlink race).
struct Cursors {
    db: Connection,
    _directory: tempfile::TempDir,
}
impl Cursors {
    fn new() -> Result<Self, ApiError> {
        let directory = tempfile::Builder::new()
            .prefix("belt-log-reader-")
            .tempdir()
            .map_err(internal)?;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .map_err(internal)?;
        let path = directory.path().join("cursor.sqlite");
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .map_err(internal)?;
        let db = Connection::open(&path).map_err(internal)?;
        db.execute_batch("PRAGMA cache_size=-256; PRAGMA mmap_size=0; PRAGMA temp_store=FILE;
            CREATE TABLE selection(task TEXT PRIMARY KEY,attempt TEXT);
            CREATE TABLE cursor(attempt TEXT,stream TEXT,delivered INTEGER NOT NULL,observed INTEGER NOT NULL,status TEXT NOT NULL,PRIMARY KEY(attempt,stream));").map_err(internal)?;
        Ok(Self {
            db,
            _directory: directory,
        })
    }
    fn select(
        &mut self,
        task: &TaskId,
        attempt: Option<&AttemptId>,
    ) -> Result<(Option<String>, bool), ApiError> {
        let previous: Option<Option<String>> = self
            .db
            .query_row(
                "SELECT attempt FROM selection WHERE task=?1",
                [task.as_str()],
                |r| r.get(0),
            )
            .optional()
            .map_err(internal)?;
        let next = attempt.map(AttemptId::as_str);
        let changed = previous.as_ref().map(|old| old.as_deref()) != Some(next);
        let replaced = previous.flatten().filter(|old| Some(old.as_str()) != next);
        let tx = self.db.transaction().map_err(internal)?;
        if let Some(old) = &replaced {
            tx.execute("DELETE FROM cursor WHERE attempt=?1", [old])
                .map_err(internal)?;
        }
        tx.execute("INSERT INTO selection(task,attempt) VALUES(?1,?2) ON CONFLICT(task) DO UPDATE SET attempt=excluded.attempt",params![task.as_str(),next]).map_err(internal)?;
        tx.commit().map_err(internal)?;
        Ok((replaced, changed))
    }
}
/// The status record of a task that has no attempt: whether it still waits,
/// why it cannot be run (`reason`, a CI task that was cancelled before it
/// was placed), and which workers it was not placed on and why
/// (`placement`). A key is there only when there is something to say.
fn waiting_status(task: &Task) -> serde_json::Value {
    let mut status = serde_json::json!({
        "kind": "status",
        "task_id": task.task_id,
        "run_id": task.run_id,
        "attempt_id": null,
        "pending": task.cancel_requested_at_ms.is_none(),
    });
    if let Some(reason) = &task.unrunnable {
        status["reason"] = serde_json::json!(reason);
    }
    if let Some(placement) = &task.placement {
        status["placement"] = serde_json::json!(placement);
    }
    status
}

/// Is what the task says about its wait news to this observer? `notes`
/// keeps what was told last per task; a task with nothing to say is
/// forgotten.
fn note_news(notes: &mut std::collections::BTreeMap<TaskId, String>, task: &Task) -> bool {
    if task.unrunnable.is_none() && task.placement.is_none() {
        notes.remove(&task.task_id);
        return false;
    }
    let note = serde_json::json!([task.unrunnable, task.placement]).to_string();
    notes.insert(task.task_id.clone(), note.clone()) != Some(note)
}

#[derive(Default, Debug, Serialize)]
pub struct Summary {
    pub tasks: u64,
    pub streams: u64,
    pub bytes: u64,
    pub pending_tasks: u64,
    pub failed_tasks: u64,
    pub undrained_streams: u64,
    pub no_work: bool,
    pub detached: bool,
}
struct Sink<'a> {
    json: bool,
    out: &'a mut (dyn AsyncWrite + Unpin),
    err: &'a mut (dyn AsyncWrite + Unpin),
}
impl Sink<'_> {
    async fn record(&mut self, data: serde_json::Value) -> Result<(), ApiError> {
        let mut bytes =
            serde_json::to_vec(&serde_json::json!({"schema_version":1,"ok":true,"data":data}))
                .map_err(internal)?;
        bytes.push(b'\n');
        self.out.write_all(&bytes).await.map_err(internal)?;
        self.out.flush().await.map_err(internal)
    }
    async fn status(&mut self, data: serde_json::Value) -> Result<(), ApiError> {
        if self.json {
            self.record(data).await
        } else {
            self.err
                .write_all(format!("{data}\n").as_bytes())
                .await
                .map_err(internal)?;
            self.err.flush().await.map_err(internal)
        }
    }
}

pub struct Observer {
    _pin: crate::results::pin::RunPin,
    reader: Reader,
    root: PathBuf,
    target: Target,
    attempt: Option<AttemptId>,
    /// What each waiting task last said about its wait (see [`note_news`]).
    notes: std::collections::BTreeMap<TaskId, String>,
    cursors: Cursors,
    delivered: u64,
    reconnect_spec: Option<super::bootstrap::Spec>,
}
impl Observer {
    pub fn new(
        reader: Reader,
        root: PathBuf,
        target: Target,
        attempt: Option<AttemptId>,
    ) -> Result<Self, ApiError> {
        if attempt.is_some() && matches!(target, Target::Run(_)) {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                "--attempt requires a task identifier",
            ));
        }
        let state = root
            .parent()
            .ok_or_else(|| internal("result state directory missing"))?;
        let metadata = crate::storage::CoordinatorStore::open_read_only(state)
            .map_err(|e| ApiError::new(e.code(), e.to_string()))?;
        let pin = metadata
            .result_read_pin(&target, attempt.as_ref())
            .map_err(|e| ApiError::new(e.code(), e.to_string()))?;
        Ok(Self {
            _pin: pin,
            reader,
            root,
            target,
            attempt,
            cursors: Cursors::new()?,
            notes: std::collections::BTreeMap::new(),
            delivered: 0,
            reconnect_spec: None,
        })
    }
    async fn completion(&self) -> Result<super::completion::Snapshot, ApiError> {
        match self
            .reader
            .query(Query::Completion {
                target: self.target.clone(),
            })
            .await
            .map_err(query_error)?
        {
            Response::Completion { snapshot } => Ok(snapshot),
            _ => Err(internal("unexpected completion response")),
        }
    }
    /// One bounded-memory sweep, one chunk per stream in follow mode. Snapshot
    /// drains the metadata snapshot's committed prefix, never a moving file EOF.
    async fn sweep(&mut self, follow: bool, sink: &mut Sink<'_>) -> Result<Summary, ApiError> {
        let mut summary = Summary::default();
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
                    _ => return Err(internal("unexpected tasks response")),
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
                    _ => return Err(internal("unexpected task response")),
                },
            };
            for task in tasks {
                summary.tasks += 1;
                self.task(task, follow, sink, &mut summary).await?;
                // Yield between targets so Ctrl-C does not wait for a huge run.
                tokio::task::yield_now().await;
            }
            after = next
                .map(|id| TaskId::parse(&id))
                .transpose()
                .map_err(internal)?;
            if after.is_none() {
                break;
            }
        }
        summary.no_work = summary.tasks == 0;
        summary.bytes = self.delivered;
        Ok(summary)
    }
    async fn task(
        &mut self,
        task: Task,
        follow: bool,
        sink: &mut Sink<'_>,
        summary: &mut Summary,
    ) -> Result<(), ApiError> {
        let aid = self
            .attempt
            .clone()
            .or_else(|| task.current_attempt.as_ref().map(|a| a.attempt_id.clone()));
        let (replaced, changed) = self.cursors.select(&task.task_id, aid.as_ref())?;
        if let Some(old) = replaced {
            sink.status(serde_json::json!({"kind":"replacement","task_id":task.task_id,"previous_attempt_id":old,"attempt_id":aid})).await?;
        }
        let Some(attempt_id) = aid else {
            // Told again when why it waits (or why it cannot be run) is news:
            // a follower that saw the task pending learns the reason.
            let news = note_news(&mut self.notes, &task);
            if changed || news {
                sink.status(waiting_status(&task)).await?;
            }
            if task.cancel_requested_at_ms.is_none() {
                summary.pending_tasks += 1;
            }
            return Ok(());
        };
        let Response::LogMetadata { metadata } = self
            .reader
            .query(Query::LogMetadata {
                task_id: task.task_id,
                attempt_id,
            })
            .await
            .map_err(query_error)?
        else {
            return Err(internal("unexpected log metadata response"));
        };
        let closed = metadata.terminal && metadata.collection_closed_at_ms.is_some();
        if !closed {
            summary.pending_tasks += 1;
        }
        let mut incomplete = false;
        for stream in [LogStream::Stdout, LogStream::Stderr] {
            let progress = match stream {
                LogStream::Stdout => &metadata.stdout,
                LogStream::Stderr => &metadata.stderr,
            };
            incomplete |= progress.as_ref().is_none_or(|p| !p.eof);
            self.stream(&metadata, stream, follow, sink, summary)
                .await?;
        }
        if closed && (incomplete || metadata.collection_verdict.as_deref() != Some("complete")) {
            summary.failed_tasks += 1;
        }
        Ok(())
    }
    async fn stream(
        &mut self,
        metadata: &LogMetadata,
        stream: LogStream,
        follow: bool,
        sink: &mut Sink<'_>,
        summary: &mut Summary,
    ) -> Result<(), ApiError> {
        let name = match stream {
            LogStream::Stdout => "stdout",
            LogStream::Stderr => "stderr",
        };
        let progress = match stream {
            LogStream::Stdout => &metadata.stdout,
            LogStream::Stderr => &metadata.stderr,
        };
        let committed = progress.as_ref().map_or(0, |p| p.committed_bytes);
        let row: Option<(i64, i64, String)> = self
            .cursors
            .db
            .query_row(
                "SELECT delivered,observed,status FROM cursor WHERE attempt=?1 AND stream=?2",
                params![metadata.attempt_id.as_str(), name],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(internal)?;
        let (offset, observed, old_status) = row.unwrap_or((0, 0, String::new()));
        let mut offset = u64::try_from(offset).map_err(internal)?;
        let observed = u64::try_from(observed).map_err(internal)?;
        if committed < observed || committed < offset {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                "committed log prefix shrank; reader detached",
            ));
        }
        let status = serde_json::json!({"kind":"status","attempt_id":metadata.attempt_id,"task_id":metadata.task_id,"run_id":metadata.run_id,"generation":metadata.generation,"stream":name,"progress":progress,"terminal":metadata.terminal,"collection_verdict":metadata.collection_verdict,"collection_closed_at_ms":metadata.collection_closed_at_ms});
        let status_text = serde_json::to_string(&status).map_err(internal)?;
        if status_text != old_status {
            sink.status(status).await?;
        }
        loop {
            if offset >= committed {
                break;
            }
            // The invocation pin excludes GC. A missing/short file with a
            // live durable reference is corruption, never an empty read.
            let reader = ResultStore::open_read_only(&self.root).map_err(internal)?;
            let bytes = reader
                .read_range(&metadata.attempt_id, stream, committed, offset, 65536)
                .map_err(internal)?;
            let next = offset + bytes.len() as u64;
            if bytes.is_empty() {
                return Err(internal("empty read within committed prefix"));
            }
            if sink.json {
                sink.record(serde_json::json!({"kind":"chunk","attempt_id":metadata.attempt_id,"task_id":metadata.task_id,"run_id":metadata.run_id,"generation":metadata.generation,"stream":name,"offset":offset,"next_offset":next,"progress":progress,"collection_closed_at_ms":metadata.collection_closed_at_ms,"bytes_base64":base64::engine::general_purpose::STANDARD.encode(&bytes)})).await?;
            } else {
                let writer: &mut (dyn AsyncWrite + Unpin) = match stream {
                    LogStream::Stdout => sink.out,
                    LogStream::Stderr => sink.err,
                };
                writer.write_all(&bytes).await.map_err(internal)?;
                writer.flush().await.map_err(internal)?;
            }
            self.delivered += bytes.len() as u64;
            offset = next;
            // Persist after delivery. Only this invocation uses the cursor, so
            // no exactly-once promise is made across a CLI process crash.
            self.cursors.db.execute("INSERT INTO cursor VALUES(?1,?2,?3,?4,?5) ON CONFLICT(attempt,stream) DO UPDATE SET delivered=excluded.delivered,observed=excluded.observed,status=excluded.status",params![metadata.attempt_id.as_str(),name,i64::try_from(offset).map_err(internal)?,i64::try_from(committed).map_err(internal)?,status_text]).map_err(internal)?;
            tokio::task::yield_now().await;
            if follow {
                break;
            }
        }
        self.cursors.db.execute("INSERT INTO cursor VALUES(?1,?2,?3,?4,?5) ON CONFLICT(attempt,stream) DO UPDATE SET observed=excluded.observed,status=excluded.status",params![metadata.attempt_id.as_str(),name,i64::try_from(offset).map_err(internal)?,i64::try_from(committed).map_err(internal)?,status_text]).map_err(internal)?;
        summary.streams += 1;
        summary.undrained_streams += u64::from(offset < committed);
        Ok(())
    }
    /// The GitHub commit status of a run target (`None` for a task target or
    /// a run that posted none).
    async fn github_status(&self) -> Result<Option<(RunId, serde_json::Value)>, ApiError> {
        let Target::Run(run_id) = &self.target else {
            return Ok(None);
        };
        match self
            .reader
            .query(Query::Run {
                run_id: run_id.clone(),
            })
            .await
            .map_err(query_error)?
        {
            Response::Run { run } => {
                Ok(run.and_then(|run| run.github_status.map(|status| (run_id.clone(), status))))
            }
            _ => Err(internal("unexpected run response")),
        }
    }

    /// Print it after the output: the `ci` line on stderr (stdout carries
    /// only command bytes), or a `status` record in `--json`.
    async fn print_github_status(
        run_id: RunId,
        status: serde_json::Value,
        sink: &mut Sink<'_>,
    ) -> Result<(), ApiError> {
        if sink.json {
            sink.record(serde_json::json!({
                "kind": "github_status", "run_id": run_id, "github_status": status
            }))
            .await
        } else {
            let record = serde_json::from_value(status).map_err(internal)?;
            sink.err
                .write_all(format!("{}\n", crate::ci::status::render(&record)).as_bytes())
                .await
                .map_err(internal)?;
            sink.err.flush().await.map_err(internal)
        }
    }

    pub async fn observe(
        &mut self,
        follow: bool,
        json: bool,
        out: &mut (dyn AsyncWrite + Unpin),
        err: &mut (dyn AsyncWrite + Unpin),
    ) -> Result<Summary, ApiError> {
        let mut sink = Sink { json, out, err };
        let mut retry_ms = 200;
        loop {
            let result: Result<_, ApiError> = async {
                let start = if follow && self.attempt.is_none() {
                    Some(self.completion().await?)
                } else {
                    None
                };
                let summary = self.sweep(follow, &mut sink).await?;
                let mut done = summary.pending_tasks == 0 && summary.undrained_streams == 0;
                if done {
                    if let Some(start) = start {
                        let end = self.completion().await?;
                        done = start.control_revision == end.control_revision
                            && end.computed_tasks == end.task_count
                            && end.collection_pending_tasks == 0;
                    }
                }
                let status = if !follow || done {
                    self.github_status().await?
                } else {
                    None
                };
                Ok((summary, done, status))
            }
            .await;
            match result {
                Ok((summary, done, status)) => {
                    retry_ms = 200;
                    if !follow || done {
                        if let Some((run_id, status)) = status {
                            Self::print_github_status(run_id, status, &mut sink).await?;
                        }
                        return Ok(summary);
                    }
                }
                Err(error)
                    if follow
                        && matches!(
                            error.code,
                            ErrorCode::Overloaded | ErrorCode::CommunicationUnknown
                        ) =>
                {
                    sink.status(serde_json::json!({"kind":"reconnecting","code":error.code,"delivered_bytes":self.delivered})).await?;
                    tokio::time::sleep(std::time::Duration::from_millis(retry_ms)).await;
                    retry_ms = (retry_ms * 2).min(2000);
                    if let Some(spec) = &self.reconnect_spec {
                        match Reader::open(spec.clone()).await {
                            Ok(reader) => self.reader = reader,
                            Err(error)
                                if matches!(
                                    error.code,
                                    ErrorCode::Overloaded | ErrorCode::CommunicationUnknown
                                ) =>
                            {
                                continue
                            }
                            Err(error) => return Err(query_error(error)),
                        }
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }
}

pub fn command(args: &crate::cli::LogsArgs, json: bool) -> i32 {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: {error}");
            return ErrorCode::Internal.exit_code();
        }
    };
    let code = runtime.block_on(command_async(args, json));
    // Tokio's blocked OS stdout/stderr jobs retain only their output buffer,
    // never coordinator ownership, cursor state or a filesystem mutation.
    runtime.shutdown_background();
    code
}

async fn command_async(args: &crate::cli::LogsArgs, json: bool) -> i32 {
    let mut signal_error = None;
    let mut interrupt = if args.follow {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()) {
            Ok(signal) => Some(signal),
            Err(error) => {
                signal_error = Some(internal(error));
                None
            }
        }
    } else {
        None
    };
    let mut out = tokio::io::stdout();
    let mut err = tokio::io::stderr();
    let mut observer = None;
    let outcome = tokio::select! {
        result=async {
            if let Some(error)=signal_error {return Err(error);}
            let target=Target::parse(&args.id)?;
            let attempt=args.attempt.as_deref().map(AttemptId::parse).transpose().map_err(|e|ApiError::new(ErrorCode::InvalidInput,e.to_string()))?;
            if attempt.is_some() && matches!(target,Target::Run(_)) {return Err(ApiError::new(ErrorCode::InvalidInput,"--attempt requires a task identifier"));}
            let env=std::env::vars_os().filter_map(|(k,v)|Some((k.into_string().ok()?,v.into_string().ok()?))).collect();
            let spec=super::context::runtime_spec(&env)?;
            let root=spec.state_dir.join("results");
            let reader=Reader::open(spec.clone()).await.map_err(query_error)?;
            let mut ready=Observer::new(reader,root,target,attempt)?;
            ready.reconnect_spec=Some(spec);
            observer=Some(ready);
            observer.as_mut().expect("observer initialized").observe(args.follow,json,&mut out,&mut err).await
        }=>Some(result),
        _=async {interrupt.as_mut().expect("follow signal installed").recv().await}, if interrupt.is_some()=>None,
    };
    let delivered = observer.as_ref().map_or(0, |observer| observer.delivered);
    let (record, code, detached) = match outcome {
        None => {
            let summary = Summary {
                bytes: delivered,
                detached: true,
                ..Summary::default()
            };
            (
                serde_json::json!({"schema_version":1,"ok":false,"error":null,"data":{"kind":"final","summary":summary,"exit_code":130}}),
                crate::error::exit::CANCELLED,
                true,
            )
        }
        Some(Ok(summary)) => {
            let error = (summary.failed_tasks > 0).then(|| {
                ApiError::new(
                    ErrorCode::CollectionFailed,
                    "collected logs are incomplete or collection failed",
                )
            });
            let code = error.as_ref().map_or(0, |error| error.code.exit_code());
            (
                serde_json::json!({"schema_version":1,"ok":code==0,"error":error,"data":{"kind":"final","summary":summary,"exit_code":code}}),
                code,
                false,
            )
        }
        Some(Err(error)) => {
            let code = error.code.exit_code();
            (
                serde_json::json!({"schema_version":1,"ok":false,"error":error,"data":{"kind":"final","summary":{"bytes":delivered,"observation_complete":false},"exit_code":code}}),
                code,
                false,
            )
        }
    };
    emit_final(
        record,
        code,
        detached,
        json,
        &mut out,
        &mut err,
        interrupt.as_mut(),
    )
    .await
}

/// One interruptible final attempt shared by public streaming readers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn emit_final<'a>(
    record: serde_json::Value,
    code: i32,
    detached: bool,
    json: bool,
    out: &'a mut (dyn AsyncWrite + Unpin),
    err: &'a mut (dyn AsyncWrite + Unpin),
    mut interrupt: Option<&mut tokio::signal::unix::Signal>,
) -> i32 {
    let bytes = format!("{record}\n");
    let writer: &mut (dyn AsyncWrite + Unpin) = if json { out } else { err };
    let output = async {
        writer.write_all(bytes.as_bytes()).await?;
        writer.flush().await
    };
    if detached {
        // A consumer that stopped reading cannot delay detach. There is only
        // one final attempt; never append a second record after a partial final.
        let _ = tokio::time::timeout(std::time::Duration::from_millis(200), output).await;
        return code;
    }
    // Keep interrupt handling alive through EVERY final/error write and flush,
    // including a closed collection whose last chunk filled the output pipe.
    tokio::select! {
        result=output=>if result.is_ok() {code}else{ErrorCode::Internal.exit_code()},
        _=async {interrupt.as_mut().expect("signal installed").recv().await}, if interrupt.is_some()=>crate::error::exit::CANCELLED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::*;
    use crate::state::{AttemptState, Outcome, Termination};
    use crate::storage::{CoordinatorStore, NewAttempt};
    use std::sync::Arc;
    struct Fixture {
        dir: tempfile::TempDir,
        db: Arc<CoordinatorStore>,
        files: ResultStore,
        run: RunId,
        task: TaskId,
        attempt: AttemptId,
    }
    impl Fixture {
        fn new() -> Self {
            let dir = crate::test_tmp::tempdir().unwrap();
            let db = Arc::new(CoordinatorStore::open(dir.path(), 1).unwrap());
            let files = ResultStore::open(&dir.path().join("results")).unwrap();
            let run = RunId::generate();
            let task = TaskId::generate();
            let attempt = AttemptId::generate();
            db.create_run(
                &run,
                None,
                None,
                &serde_json::json!({"secret":"never_print_me"}),
                1,
            )
            .unwrap();
            db.create_task(&task, &run, &serde_json::json!({}), 1)
                .unwrap();
            db.create_attempt(
                NewAttempt {
                    attempt_id: attempt.clone(),
                    task_id: task.clone(),
                    worker_id: WorkerId::parse("test").unwrap(),
                    payload_hash: Digest::of_bytes(b"payload"),
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
            ] {
                db.transition_attempt(&attempt, state, 3).unwrap();
            }
            Self {
                dir,
                db,
                files,
                run,
                task,
                attempt,
            }
        }
        fn observer(&self, run: bool) -> Observer {
            Observer::new(
                Reader::Offline(Arc::new(
                    CoordinatorStore::open_read_only(self.dir.path()).unwrap(),
                )),
                self.dir.path().join("results"),
                if run {
                    Target::Run(self.run.clone())
                } else {
                    Target::Task(self.task.clone())
                },
                None,
            )
            .unwrap()
        }
        fn append(&self, stream: LogStream, bytes: &[u8], eof: bool) {
            let old = self
                .db
                .prepare_log_progress(&self.attempt, stream, 3)
                .unwrap()
                .committed_bytes;
            self.files
                .append_log(&self.attempt, stream, old, bytes)
                .unwrap();
            assert!(self
                .db
                .commit_log_progress(
                    &self.attempt,
                    stream,
                    old,
                    old + bytes.len() as u64,
                    eof,
                    false,
                    0,
                    4
                )
                .unwrap());
        }
        fn terminal(&self, closed: bool) {
            self.db
                .transition_attempt(&self.attempt, AttemptState::Finalizing, 5)
                .unwrap();
            self.db
                .finalize_attempt(
                    &self.attempt,
                    Termination::new(Outcome::Succeeded, None, None).unwrap(),
                    6,
                )
                .unwrap();
            if closed {
                self.db
                    .close_collection(
                        &self.attempt,
                        None,
                        crate::results::plan::Verdict::GaveUp,
                        false,
                        7,
                    )
                    .unwrap();
            }
        }
    }
    #[test]
    fn a_waiting_task_says_why_it_waits_or_cannot_run_once_per_change() {
        use crate::scheduler::decide::{Excluded, Exclusion};
        use crate::storage::query::PlacementDiagnostics;
        let mut task = Task {
            unrunnable: None,
            placement: None,
            in_place: false,
            task_id: TaskId::generate(),
            run_id: RunId::generate(),
            created_at_ms: 1,
            cancel_requested_at_ms: None,
            current_attempt: None,
        };
        let mut notes = std::collections::BTreeMap::new();
        let plain = waiting_status(&task);
        assert_eq!(plain["kind"], "status");
        assert_eq!(plain["pending"], true);
        assert!(plain["attempt_id"].is_null());
        assert!(plain.get("reason").is_none() && plain.get("placement").is_none());
        assert!(!note_news(&mut notes, &task));
        assert!(notes.is_empty());

        // A worker did not take the plan of the run.
        task.placement = Some(PlacementDiagnostics {
            blocked_reason: None,
            excluded: vec![Excluded {
                unmet: vec!["frozen plan not taken".into()],
                worker_id: crate::ids::WorkerId::parse("w-a").unwrap(),
                why: Exclusion::InputRefused,
            }],
            truncated: false,
        });
        let waiting = waiting_status(&task);
        assert_eq!(waiting["placement"]["excluded"][0]["why"], "input_refused");
        assert_eq!(
            waiting["placement"]["excluded"][0]["unmet"][0],
            "frozen plan not taken"
        );
        assert!(waiting.get("reason").is_none());
        assert!(note_news(&mut notes, &task));
        assert!(!note_news(&mut notes, &task), "told once");

        // It was cancelled because its run cannot go on: news again.
        task.placement = None;
        task.unrunnable = Some("the frozen plan is gone".into());
        task.cancel_requested_at_ms = Some(2);
        let ended = waiting_status(&task);
        assert_eq!(ended["reason"], "the frozen plan is gone");
        assert_eq!(ended["pending"], false);
        assert!(ended.get("placement").is_none());
        assert!(note_news(&mut notes, &task));
        assert!(!note_news(&mut notes, &task));
        // Another task is told on its own.
        let other = Task {
            task_id: TaskId::generate(),
            ..task.clone()
        };
        assert!(note_news(&mut notes, &other));
        assert_eq!(notes.len(), 2);
        // Nothing to say any more: forgotten, so the same reason later is news.
        task.unrunnable = None;
        assert!(!note_news(&mut notes, &task));
        assert_eq!(notes.len(), 1);
        task.unrunnable = Some("the frozen plan is gone".into());
        assert!(note_news(&mut notes, &task));
    }

    #[tokio::test]
    async fn run_without_github_status_adds_no_output() {
        let f = Fixture::new();
        f.append(LogStream::Stderr, b"last command output\n", true);
        f.terminal(true);
        for follow in [false, true] {
            for json in [false, true] {
                let (mut out, mut err) = (Vec::new(), Vec::new());
                f.observer(true)
                    .observe(follow, json, &mut out, &mut err)
                    .await
                    .unwrap();
                if json {
                    assert!(!String::from_utf8(out).unwrap().contains("github_status"));
                    assert!(err.is_empty());
                } else {
                    assert!(out.is_empty());
                    assert!(err.ends_with(b"last command output\n"));
                }
            }
        }
    }

    #[tokio::test]
    async fn follow_reads_github_status_after_the_run_ends() {
        let f = Fixture::new();
        f.append(LogStream::Stdout, b"before\n", false);
        let mut observer = f.observer(true);
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let (result, ()) = tokio::join!(observer.observe(true, false, &mut out, &mut err), async {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            f.append(LogStream::Stderr, b"after\n", true);
            f.db.record_github_status(
                &f.run,
                &serde_json::json!({
                    "computed": "failure", "posting": "failed", "sha": "0123456789abcdef",
                    "state": "failure", "context": "local-ci", "error": "unavailable"
                }),
                9,
            )
            .unwrap();
            f.terminal(true);
        });
        result.unwrap();
        assert_eq!(out, b"before\n");
        let err = String::from_utf8(err).unwrap();
        assert!(
            err.ends_with(
                "after\nstatus: local-ci=failure could not be posted to 0123456 (unavailable)\n"
            ),
            "{err}"
        );
        assert_eq!(err.matches("status: local-ci=").count(), 1);
    }

    #[tokio::test]
    async fn github_status_follows_run_logs_once_in_text_and_json() {
        let f = Fixture::new();
        f.append(LogStream::Stdout, b"command output\n", true);
        f.append(LogStream::Stderr, b"command error\n", true);
        f.terminal(true);
        let record = serde_json::json!({
            "computed": "success", "posting": "skipped", "reason": "--no-status",
            "source_digest": "digest", "original_head": "0123456789abcdef"
        });
        f.db.record_github_status(&f.run, &record, 9).unwrap();
        for follow in [false, true] {
            for json in [false, true] {
                let (mut out, mut err) = (Vec::new(), Vec::new());
                f.observer(true)
                    .observe(follow, json, &mut out, &mut err)
                    .await
                    .unwrap();
                if json {
                    let records: Vec<serde_json::Value> = out
                        .split(|b| *b == b'\n')
                        .filter(|line| !line.is_empty())
                        .map(|line| serde_json::from_slice(line).unwrap())
                        .collect();
                    assert_eq!(
                        records.last().unwrap()["data"],
                        serde_json::json!({
                            "kind": "github_status", "run_id": f.run, "github_status": record
                        })
                    );
                    assert_eq!(
                        records
                            .iter()
                            .filter(|r| !r["data"]["github_status"].is_null())
                            .count(),
                        1
                    );
                    assert!(err.is_empty());
                } else {
                    assert_eq!(out, b"command output\n");
                    let err = String::from_utf8(err).unwrap();
                    assert!(
                        err.ends_with("command error\nstatus: not posted (--no-status)\n"),
                        "{err}"
                    );
                    assert_eq!(err.matches("status: not posted").count(), 1);
                }
            }
        }
        let (mut out, mut err) = (Vec::new(), Vec::new());
        f.observer(false)
            .observe(false, false, &mut out, &mut err)
            .await
            .unwrap();
        assert!(!String::from_utf8(err)
            .unwrap()
            .contains("status: not posted"));
    }

    #[tokio::test]
    async fn snapshot_raw_bytes_prefix_boundary_ndjson_and_readonly() {
        let f = Fixture::new();
        let epoch = f.db.epoch();
        let mut raw = vec![b'x'; 65535];
        raw.extend_from_slice(b"\xe3\x81\x82\0\xff");
        f.append(LogStream::Stdout, &raw, false);
        f.append(LogStream::Stderr, b"\xfe\0", false);
        f.files
            .append_log(
                &f.attempt,
                LogStream::Stdout,
                raw.len() as u64,
                b"invisible",
            )
            .unwrap();
        let mut observer = f.observer(false);
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let summary = observer
            .observe(false, false, &mut out, &mut err)
            .await
            .unwrap();
        assert_eq!(out, raw);
        assert!(err.windows(2).any(|b| b == b"\xfe\0"));
        assert_eq!(summary.pending_tasks, 1);
        let mut observer = f.observer(false);
        out.clear();
        err.clear();
        observer
            .observe(false, true, &mut out, &mut err)
            .await
            .unwrap();
        let records = out
            .split(|b| *b == b'\n')
            .filter(|s| !s.is_empty())
            .map(|s| serde_json::from_slice::<serde_json::Value>(s).unwrap())
            .collect::<Vec<_>>();
        let mut decoded = Vec::new();
        let mut offset = 0;
        for record in records
            .iter()
            .filter(|r| r["data"]["kind"] == "chunk" && r["data"]["stream"] == "stdout")
        {
            let data = &record["data"];
            assert_eq!(data["offset"], offset);
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(data["bytes_base64"].as_str().unwrap())
                .unwrap();
            assert!(bytes.len() <= 65536);
            offset += bytes.len() as u64;
            assert_eq!(data["next_offset"], offset);
            decoded.extend(bytes);
            assert!(
                serde_json::to_vec(record).unwrap().len()
                    < super::super::resident::MAX_REQUEST_BYTES
            );
        }
        assert_eq!(decoded, raw);
        assert!(err.is_empty());
        assert!(!String::from_utf8(out).unwrap().contains("never_print_me"));
        assert_eq!(f.db.epoch(), epoch);
    }
    #[tokio::test]
    async fn follow_sweeps_resume_without_duplicates_and_close_incomplete() {
        let f = Fixture::new();
        f.append(LogStream::Stdout, b"first", false);
        let mut observer = f.observer(false);
        let (mut out, mut err) = (Vec::new(), Vec::new());
        {
            let mut sink = Sink {
                json: false,
                out: &mut out,
                err: &mut err,
            };
            let s = observer.sweep(true, &mut sink).await.unwrap();
            assert_eq!(s.pending_tasks, 1);
        }
        f.append(LogStream::Stdout, b"\xffsecond", false);
        f.terminal(false);
        {
            let mut sink = Sink {
                json: false,
                out: &mut out,
                err: &mut err,
            };
            let s = observer.sweep(true, &mut sink).await.unwrap();
            assert_eq!(s.pending_tasks, 1);
        }
        f.db.close_collection(
            &f.attempt,
            None,
            crate::results::plan::Verdict::GaveUp,
            false,
            8,
        )
        .unwrap();
        let summary = observer
            .observe(true, false, &mut out, &mut err)
            .await
            .unwrap();
        assert_eq!(out, b"first\xffsecond");
        assert_eq!(summary.pending_tasks, 0);
        assert_eq!(summary.failed_tasks, 1);
    }
    #[tokio::test]
    async fn published_file_missing_or_short_is_explicit_error() {
        for missing in [true, false] {
            let f = Fixture::new();
            f.append(LogStream::Stdout, b"committed", false);
            let path = f
                .dir
                .path()
                .join("results")
                .join(f.attempt.as_str())
                .join("logs/stdout.log");
            if missing {
                std::fs::remove_file(path).unwrap();
            } else {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_len(1)
                    .unwrap();
            }
            let error = f
                .observer(false)
                .observe(false, true, &mut Vec::new(), &mut Vec::new())
                .await
                .unwrap_err();
            assert_eq!(error.code, ErrorCode::Internal);
        }
    }
    #[tokio::test]
    async fn retry_boundary_hides_history_and_historical_membership_is_checked() {
        let f = Fixture::new();
        f.append(LogStream::Stdout, b"old", false);
        let mut observer = f.observer(false);
        let (mut out, mut err) = (Vec::new(), Vec::new());
        {
            let mut sink = Sink {
                json: true,
                out: &mut out,
                err: &mut err,
            };
            observer.sweep(true, &mut sink).await.unwrap();
        }
        let task = f.task.to_string();
        f.db.db
            .write(move |tx| {
                tx.execute("UPDATE task SET min_generation=2 WHERE task_id=?1", [task])?;
                Ok(())
            })
            .unwrap();
        out.clear();
        let summary = observer
            .observe(false, true, &mut out, &mut err)
            .await
            .unwrap();
        assert_eq!(summary.pending_tasks, 1);
        assert!(!String::from_utf8(out).unwrap().contains("bytes_base64"));
        let mut history = f.observer(false);
        history.attempt = Some(f.attempt.clone());
        out = Vec::new();
        history
            .observe(false, false, &mut out, &mut err)
            .await
            .unwrap();
        assert_eq!(out, b"old");
        let other = TaskId::generate();
        f.db.create_task(&other, &f.run, &serde_json::json!({}), 8)
            .unwrap();
        history.target = Target::Task(other);
        let e = history
            .observe(false, false, &mut Vec::new(), &mut Vec::new())
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
    }
    #[tokio::test]
    async fn committed_prefix_regression_is_conflict() {
        let f = Fixture::new();
        f.append(LogStream::Stdout, b"abcdef", false);
        let mut observer = f.observer(false);
        {
            let mut sink = Sink {
                json: true,
                out: &mut Vec::new(),
                err: &mut Vec::new(),
            };
            observer.sweep(true, &mut sink).await.unwrap();
        }
        let aid = f.attempt.to_string();
        f.db.db
            .write(move |tx| {
                tx.execute(
                    "UPDATE collection_log SET committed_bytes=1 WHERE attempt_id=?1",
                    [aid],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            observer
                .observe(false, true, &mut Vec::new(), &mut Vec::new())
                .await
                .unwrap_err()
                .code,
            ErrorCode::Conflict
        );
    }
    #[tokio::test]
    async fn pages_over_64_and_big_streams_are_fair_and_cursor_is_private() {
        let f = Fixture::new();
        f.append(LogStream::Stdout, &vec![0xff; 131073], false);
        for _ in 0..70 {
            f.db.create_task(&TaskId::generate(), &f.run, &serde_json::json!({}), 9)
                .unwrap();
        }
        let mut observer = f.observer(true);
        let mut out = Vec::new();
        let mut err = Vec::new();
        {
            let mut sink = Sink {
                json: false,
                out: &mut out,
                err: &mut err,
            };
            let s = observer.sweep(true, &mut sink).await.unwrap();
            assert_eq!(s.tasks, 71);
            assert_eq!(s.undrained_streams, 1);
            assert_eq!(out.len(), 65536);
        }
        {
            let mut sink = Sink {
                json: false,
                out: &mut out,
                err: &mut err,
            };
            observer.sweep(true, &mut sink).await.unwrap();
        }
        assert_eq!(out.len(), 131072);
        let path = observer.cursors._directory.path().to_owned();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(path.join("cursor.sqlite"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let count: i64 = observer
            .cursors
            .db
            .query_row("SELECT count(*) FROM selection", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 71);
        drop(observer);
        assert!(!path.exists());
    }
    #[tokio::test]
    async fn absent_result_root_is_not_created_and_no_work_finishes() {
        let f = Fixture::new();
        let root = f.dir.path().join("absent");
        let mut observer = f.observer(false);
        observer.root = root.clone();
        let s = observer
            .observe(false, true, &mut Vec::new(), &mut Vec::new())
            .await
            .unwrap();
        assert_eq!(s.pending_tasks, 1);
        assert!(!root.exists());
        let empty = RunId::generate();
        f.db.create_run(&empty, None, None, &serde_json::json!({}), 8)
            .unwrap();
        observer.target = Target::Run(empty);
        assert!(
            observer
                .observe(true, true, &mut Vec::new(), &mut Vec::new())
                .await
                .unwrap()
                .no_work
        );
    }
    #[tokio::test]
    async fn new_generation_emits_replacement_without_replaying_old_bytes() {
        let f = Fixture::new();
        f.append(LogStream::Stdout, b"old", false);
        let mut observer = f.observer(false);
        let (mut out, mut err) = (Vec::new(), Vec::new());
        {
            let mut sink = Sink {
                json: true,
                out: &mut out,
                err: &mut err,
            };
            observer.sweep(true, &mut sink).await.unwrap();
        }
        f.terminal(true);
        let next = AttemptId::generate();
        f.db.create_attempt(
            NewAttempt {
                attempt_id: next.clone(),
                task_id: f.task.clone(),
                worker_id: WorkerId::parse("test").unwrap(),
                payload_hash: Digest::of_bytes(b"next"),
                permit_id: None,
            },
            9,
        )
        .unwrap();
        f.db.prepare_log_progress(&next, LogStream::Stdout, 9)
            .unwrap();
        f.files
            .append_log(&next, LogStream::Stdout, 0, b"new")
            .unwrap();
        f.db.commit_log_progress(&next, LogStream::Stdout, 0, 3, false, false, 0, 10)
            .unwrap();
        out.clear();
        observer
            .observe(false, true, &mut out, &mut err)
            .await
            .unwrap();
        let records = out
            .split(|b| *b == b'\n')
            .filter(|s| !s.is_empty())
            .map(|s| serde_json::from_slice::<serde_json::Value>(s).unwrap())
            .collect::<Vec<_>>();
        let replacement = records
            .iter()
            .find(|r| r["data"]["kind"] == "replacement")
            .unwrap();
        assert_eq!(
            replacement["data"]["previous_attempt_id"],
            f.attempt.as_str()
        );
        assert_eq!(replacement["data"]["attempt_id"], next.as_str());
        let chunk = records
            .iter()
            .find(|r| r["data"]["kind"] == "chunk")
            .unwrap();
        assert_eq!(chunk["data"]["offset"], 0);
        assert_eq!(chunk["data"]["bytes_base64"], "bmV3");
        assert_eq!(chunk["data"]["generation"], 2);
    }
    #[tokio::test]
    async fn legacy_final_receipt_is_bounded_readonly_metadata() {
        let f = Fixture::new();
        f.terminal(false);
        let digest = Digest::of_bytes(b"result");
        f.db.begin_collection(
            &f.attempt,
            &digest,
            &serde_json::json!({}),
            &[],
            &[crate::storage::ResultRef {
                ref_id: format!("{}:logs", f.attempt),
                kind: crate::storage::BlobKind::Log,
                locator: ResultStore::logs_locator(&f.attempt),
            }],
            7,
        )
        .unwrap();
        f.files
            .append_log(&f.attempt, LogStream::Stdout, 0, b"oldTAIL")
            .unwrap();
        f.db.set_collection_logs(&f.attempt,&digest,&serde_json::json!({"stdout":{"sha256":Digest::of_bytes(b"old"),"bytes":3,"complete":true,"log_truncated":false,"dropped_bytes":0},"stderr":{"sha256":Digest::of_bytes(b""),"bytes":0,"complete":true,"log_truncated":false,"dropped_bytes":0},"ignored": "private_metadata"}),8).unwrap();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        f.observer(false)
            .observe(false, false, &mut out, &mut err)
            .await
            .unwrap();
        assert_eq!(out, b"old");
        assert!(err.len() < 4096);
        assert!(f
            .db
            .log_progress(&f.attempt, LogStream::Stdout)
            .unwrap()
            .is_none());
        f.db.set_collection_logs(
            &f.attempt,
            &digest,
            &serde_json::json!({"ignored":"x".repeat(1024*1024)}),
            9,
        )
        .unwrap();
        let error = f
            .observer(false)
            .observe(false, true, &mut Vec::new(), &mut Vec::new())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Internal);
    }
    #[test]
    fn parser_keeps_existing_logs_grammar_and_attempt_validation_precedes_state() {
        use clap::Parser;
        assert!(
            crate::cli::Cli::try_parse_from(["belt", "logs", "task_invalid", "--follow"]).is_ok()
        );
        assert!(crate::cli::Cli::try_parse_from([
            "belt",
            "logs",
            "task_invalid",
            "--attempt",
            "attempt_invalid"
        ])
        .is_ok());
        assert!(crate::cli::Cli::try_parse_from([
            "belt",
            "logs",
            "task_invalid",
            "--stream",
            "stderr"
        ])
        .is_err());
        let args = crate::cli::LogsArgs {
            id: RunId::generate().to_string(),
            attempt: Some(AttemptId::generate().to_string()),
            follow: false,
        };
        assert_eq!(command(&args, true), ErrorCode::InvalidInput.exit_code());
    }
    #[tokio::test]
    async fn gc_claimed_log_prefix_is_unavailable_even_before_file_deletion() {
        let f = Fixture::new();
        f.append(LogStream::Stdout, b"retained", true);
        f.terminal(true);
        let guard = crate::test_tmp::gc_guard(&f.db, &f.run);
        assert!(
            f.db.claim_result_page(&guard, &f.run, "", 64, 100, 10, Some("boot".into()), 10)
                .unwrap()
                .clock_held
        );
        assert_eq!(
            f.db.claim_result_page(&guard, &f.run, "", 64, 110, 20, Some("boot".into()), 10)
                .unwrap()
                .removed,
            1
        );
        drop(guard);
        let error = f
            .observer(false)
            .observe(false, true, &mut Vec::new(), &mut Vec::new())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::NotFound);
        let guard = crate::test_tmp::gc_guard(&f.db, &f.run);
        for claim in f.db.pending_result_page(&guard, &f.run, 64).unwrap() {
            f.files.remove(&claim.locator).unwrap();
            f.db.finish_result_delete(&guard, &claim).unwrap();
        }
        drop(guard);
        assert_eq!(
            f.db.query_log_progress(&f.attempt, LogStream::Stdout)
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
    }
}
