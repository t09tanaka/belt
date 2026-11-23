//! Public metadata-only query protocol; pagination never embeds execution inputs.
use crate::ids::*;
use crate::state::{AttemptState, Observation, Receipts, Termination};
use crate::storage::query::{Page, Run, Task};
use crate::storage::{CoordinatorStore, Result};
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "query", rename_all = "snake_case", deny_unknown_fields)]
pub enum Query {
    OutputMetadata {
        task_id: TaskId,
        attempt_id: AttemptId,
        after: Option<String>,
        limit: u32,
    },
    LogMetadata {
        task_id: TaskId,
        attempt_id: AttemptId,
    },
    Completion {
        target: super::completion::Target,
    },
    Runs {
        limit: u32,
        after: Option<RunId>,
    },
    Run {
        run_id: RunId,
    },
    Tasks {
        run_id: RunId,
        limit: u32,
        after: Option<TaskId>,
    },
    Task {
        task_id: TaskId,
    },
    Attempts {
        task_id: TaskId,
        limit: u32,
        after: Option<Generation>,
    },
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Attempt {
    pub attempt_id: AttemptId,
    pub task_id: TaskId,
    pub worker_id: WorkerId,
    pub generation: Generation,
    pub state: AttemptState,
    pub observation: Observation,
    pub termination: Option<Termination>,
    pub receipts: Receipts,
    pub retired: bool,
    pub cancel_requested_at_ms: Option<u64>,
    pub collection_failed_at_ms: Option<u64>,
    pub created_at_ms: u64,
    pub terminal_at_ms: Option<u64>,
    /// None until the collection has closed; terminal execution alone is not complete.
    pub collection_verdict: Option<String>,
    pub collection_closed_at_ms: Option<u64>,
}
/// Whitelisted log identity and durable stream facts; no payload or node input.
#[derive(Debug, Serialize, Deserialize)]
pub struct LogMetadata {
    pub attempt_id: AttemptId,
    pub task_id: TaskId,
    pub run_id: RunId,
    pub generation: Generation,
    pub terminal: bool,
    pub collection_verdict: Option<String>,
    pub collection_closed_at_ms: Option<u64>,
    pub stdout: Option<crate::storage::LogProgress>,
    pub stderr: Option<crate::storage::LogProgress>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Response {
    OutputMetadata {
        page: Page<crate::storage::query::Output>,
    },
    LogMetadata {
        metadata: Box<LogMetadata>,
    },
    Completion {
        snapshot: super::completion::Snapshot,
    },
    Runs {
        page: Page<Run>,
    },
    Run {
        run: Option<Run>,
    },
    Tasks {
        page: Page<Task>,
    },
    Task {
        task: Option<Task>,
    },
    Attempts {
        page: Page<Attempt>,
    },
}
impl CoordinatorStore {
    /// An answer that is put together from several reads of a task (its
    /// attempts and their collections, its logs, its outputs) is given only
    /// when, after the last of them, the task has its row and its run has
    /// its row and no mark of a pruned run (spec 25.3 (b)): the mark is
    /// never cleared and rows are deleted only after it, so every read
    /// before saw the run whole. Otherwise the answer is "not found", and
    /// nothing that was read of a run that was being deleted is used.
    ///
    /// That holds for a refusal too: an error that was read of a task whose
    /// row was gone ("attempt does not belong to task") says something of
    /// rows that were being deleted, and is "not found" as well.
    pub fn query(&self, query: Query) -> Result<Response> {
        self.query_after(query, || ())
    }
    /// [`Self::query`]; `between` runs after the reads and before the check
    /// that follows the last of them (a test prunes the run there).
    pub(crate) fn query_after(&self, query: Query, between: impl FnOnce()) -> Result<Response> {
        let task = match &query {
            Query::OutputMetadata { task_id, .. }
            | Query::LogMetadata { task_id, .. }
            | Query::Attempts { task_id, .. } => Some(task_id.clone()),
            _ => None,
        };
        let answer = self.query_rows(query);
        between();
        if let Some(task) = task {
            self.refuse_pruned_task_read(&task)?;
            if !self.task_is_whole(&task)? {
                return Err(crate::storage::StorageError::NotFound {
                    what: format!("task {task}"),
                });
            }
        }
        answer
    }
    fn query_rows(&self, query: Query) -> Result<Response> {
        Ok(match query {
            Query::OutputMetadata {
                task_id,
                attempt_id,
                after,
                limit,
            } => {
                self.refuse_pruned_task_read(&task_id)?;
                let a = self.get_attempt(&attempt_id)?.ok_or_else(|| {
                    crate::storage::StorageError::NotFound {
                        what: "attempt".into(),
                    }
                })?;
                if a.task_id != task_id {
                    return Err(crate::storage::StorageError::Invalid(
                        "attempt does not belong to task".into(),
                    ));
                }
                Response::OutputMetadata {
                    page: self.query_output_metadata(&attempt_id, after, limit)?,
                }
            }
            Query::LogMetadata {
                task_id,
                attempt_id,
            } => {
                let task = self.query_task(task_id.clone())?.ok_or_else(|| {
                    crate::storage::StorageError::NotFound {
                        what: format!("task {task_id}"),
                    }
                })?;
                let attempt = self.get_attempt(&attempt_id)?.ok_or_else(|| {
                    crate::storage::StorageError::NotFound {
                        what: format!("attempt {attempt_id}"),
                    }
                })?;
                if attempt.task_id != task_id {
                    return Err(crate::storage::StorageError::Invalid(
                        "attempt does not belong to task".into(),
                    ));
                }
                let (collection_verdict, collection_closed_at_ms) =
                    self.query_collection_metadata(&attempt_id)?;
                Response::LogMetadata {
                    metadata: Box::new(LogMetadata {
                        stdout: self
                            .query_log_progress(&attempt_id, crate::wire::LogStream::Stdout)?,
                        stderr: self
                            .query_log_progress(&attempt_id, crate::wire::LogStream::Stderr)?,
                        attempt_id,
                        task_id,
                        run_id: task.run_id,
                        generation: attempt.generation,
                        terminal: attempt.termination.is_some(),
                        collection_verdict,
                        collection_closed_at_ms,
                    }),
                }
            }
            Query::Completion { target } => Response::Completion {
                snapshot: self.completion(target)?,
            },
            Query::Runs { limit, after } => Response::Runs {
                page: self.query_runs(limit, after)?,
            },
            Query::Run { run_id } => Response::Run {
                run: self.query_run(run_id)?,
            },
            Query::Tasks {
                run_id,
                limit,
                after,
            } => Response::Tasks {
                page: self.query_tasks(run_id, limit, after)?,
            },
            Query::Task { task_id } => Response::Task {
                task: self.query_task(task_id)?,
            },
            Query::Attempts {
                task_id,
                limit,
                after,
            } => {
                let page = self.query_attempts(task_id, limit, after)?;
                let items = page
                    .items
                    .into_iter()
                    .map(|a| {
                        let (verdict, closed) = self.query_collection_metadata(&a.attempt_id)?;
                        Ok(Attempt {
                            attempt_id: a.attempt_id,
                            task_id: a.task_id,
                            worker_id: a.worker_id,
                            generation: a.generation,
                            state: a.state,
                            observation: a.observation,
                            termination: a.termination,
                            receipts: a.receipts,
                            retired: a.retired,
                            cancel_requested_at_ms: a.cancel_requested_at_ms,
                            collection_failed_at_ms: a.collection_failed_at_ms,
                            created_at_ms: a.created_at_ms,
                            terminal_at_ms: a.terminal_at_ms,
                            collection_verdict: verdict,
                            collection_closed_at_ms: closed,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Response::Attempts {
                    page: Page {
                        items,
                        next_cursor: page.next_cursor,
                    },
                }
            }
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    pub code: crate::error::ErrorCode,
    pub message: String,
}
impl Error {
    pub(crate) fn new(code: crate::error::ErrorCode, message: impl ToString) -> Self {
        Self {
            code,
            message: message.to_string(),
        }
    }
}
impl From<crate::ids::IdError> for Error {
    fn from(e: crate::ids::IdError) -> Self {
        Self::new(crate::error::ErrorCode::InvalidInput, e)
    }
}
impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Self {
        Self::new(crate::error::ErrorCode::InvalidInput, e)
    }
}
/// A bounded show response. `next_cursor` applies to tasks of a run or the
/// attempt generations of a task; callers must follow it before assuming the
/// displayed history is complete.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Show {
    Run { run: Run, tasks: Page<Task> },
    Task { task: Task, attempts: Page<Attempt> },
}
/// Read-only durable access is available with the owner stopped. Opening this
/// handle never starts a daemon, takes ownership, or migrates the database.
pub enum Reader {
    Connected(super::resident::Client),
    Offline(std::sync::Arc<CoordinatorStore>),
}
impl Reader {
    pub async fn open(spec: super::bootstrap::Spec) -> std::result::Result<Self, Error> {
        let store = tokio::task::spawn_blocking(move || super::bootstrap::read_only_store(&spec))
            .await
            .map_err(|e| Error::new(crate::error::ErrorCode::Internal, e))?
            .map_err(|e| Error::new(crate::error::ErrorCode::InvalidInput, e))?;
        Ok(Self::Offline(std::sync::Arc::new(store)))
    }
    pub async fn query(&self, query: Query) -> std::result::Result<Response, Error> {
        match self {
            Self::Connected(client) => client.query(query).await,
            Self::Offline(store) => {
                let store = store.clone();
                tokio::task::spawn_blocking(move || store.query(query))
                    .await
                    .map_err(|e| Error::new(crate::error::ErrorCode::Internal, e))?
                    .map_err(|e| {
                        let code = if e.is_retryable() {
                            crate::error::ErrorCode::Overloaded
                        } else {
                            e.code()
                        };
                        Error::new(code, e)
                    })
            }
        }
    }
    pub async fn show(
        &self,
        id: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> std::result::Result<Show, Error> {
        if id.starts_with("run_") {
            let run_id = RunId::parse(id)?;
            let after = cursor.map(TaskId::parse).transpose()?;
            let Response::Run { run: Some(run) } = self
                .query(Query::Run {
                    run_id: run_id.clone(),
                })
                .await?
            else {
                return Err(Error::new(
                    crate::error::ErrorCode::NotFound,
                    format!("run {run_id} not found ({})", crate::storage::RUN_RETENTION),
                ));
            };
            let Response::Tasks { page: tasks } = self
                .query(Query::Tasks {
                    run_id,
                    limit,
                    after,
                })
                .await?
            else {
                return Err(Error::new(
                    crate::error::ErrorCode::Internal,
                    "unexpected tasks response",
                ));
            };
            Ok(Show::Run { run, tasks })
        } else if id.starts_with("task_") {
            let task_id = TaskId::parse(id)?;
            let after = cursor.map(str::parse::<u64>).transpose()?.map(Generation);
            let Response::Task { task: Some(task) } = self
                .query(Query::Task {
                    task_id: task_id.clone(),
                })
                .await?
            else {
                return Err(Error::new(
                    crate::error::ErrorCode::NotFound,
                    format!("task {task_id} not found"),
                ));
            };
            let Response::Attempts { page: attempts } = self
                .query(Query::Attempts {
                    task_id,
                    limit,
                    after,
                })
                .await?
            else {
                return Err(Error::new(
                    crate::error::ErrorCode::Internal,
                    "unexpected attempts response",
                ));
            };
            Ok(Show::Task { task, attempts })
        } else {
            Err(Error::new(
                crate::error::ErrorCode::InvalidInput,
                "expected run_ or task_ identifier",
            ))
        }
    }
}

impl super::resident::Client {
    pub async fn show(
        &self,
        id: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> std::result::Result<Show, Error> {
        Reader::Connected(self.clone())
            .show(id, limit, cursor)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    /// Spec 25.3 (b). `show` of a run that is being pruned, and of a task of
    /// it, answers "not found" with all its rows and with a part of them;
    /// `ls` does not list it. A run without the mark is shown as before.
    #[tokio::test]
    async fn show_and_ls_do_not_find_a_run_that_is_being_pruned() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = CoordinatorStore::open(dir.path(), 1).unwrap();
        let (run, first, second) = (RunId::generate(), TaskId::generate(), TaskId::generate());
        let spec = serde_json::json!({});
        store.create_run(&run, None, None, &spec, 1).unwrap();
        store.create_task(&first, &run, &spec, 1).unwrap();
        store.create_task(&second, &run, &spec, 1).unwrap();
        let reader = Reader::Offline(std::sync::Arc::new(
            CoordinatorStore::open_read_only(dir.path()).unwrap(),
        ));
        async fn listed(reader: &Reader) -> usize {
            let query = Query::Runs {
                limit: 10,
                after: None,
            };
            match reader.query(query).await.unwrap() {
                Response::Runs { page } => page.items.len(),
                _ => unreachable!(),
            }
        }
        assert!(matches!(
            reader.show(run.as_str(), 10, None).await.unwrap(),
            Show::Run { tasks, .. } if tasks.items.len() == 2
        ));
        assert!(reader.show(first.as_str(), 10, None).await.is_ok());
        assert_eq!(listed(&reader).await, 1);

        store.mark_pruning_for_test(&run);
        for delete in [None, Some(&second)] {
            if let Some(task) = delete {
                store.delete_task_for_test(task);
            }
            for id in [run.as_str(), first.as_str()] {
                let error = reader.show(id, 10, None).await.err().unwrap();
                assert_eq!(error.code, ErrorCode::NotFound, "{}", error.message);
            }
            assert_eq!(listed(&reader).await, 0);
        }
    }
}
