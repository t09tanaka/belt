//! Authenticated control and read-only waiting. No CLI database writes or worker signals.
use super::{
    completion::{Snapshot, Target},
    context::Context,
    query::{Query, Response},
    resident::{Operation, Reply},
};
use crate::{
    error::{ApiError, ErrorCode},
    ids::*,
};
use serde::Serialize;
use std::time::Duration;
use tokio::sync::mpsc;
#[derive(Debug, Serialize)]
pub struct Report {
    pub schema_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_receipt: Option<super::completion::RetryReceipt>,
    pub target: Target,
    pub task_id: Option<TaskId>,
    pub state: &'static str,
    pub stop_confirmed: bool,
    pub collection_pending: Option<bool>,
    pub cancel_requested: bool,
    pub cancel_acknowledged: bool,
    pub snapshot: Option<Snapshot>,
    pub cli_exit_code: i32,
    pub error: Option<ApiError>,
}
async fn interrupted(signals: &mut mpsc::UnboundedReceiver<()>) {
    if signals.recv().await.is_none() {
        std::future::pending::<()>().await;
    }
}
fn report(target: Target, snapshot: Option<Snapshot>, state: &'static str, exit: i32) -> Report {
    Report {
        schema_version: 1,
        retry_receipt: None,
        target,
        task_id: None,
        state,
        stop_confirmed: snapshot.as_ref().is_some_and(Snapshot::stop_confirmed),
        collection_pending: snapshot.as_ref().map(|s| s.collection_pending_tasks > 0),
        cancel_requested: false,
        cancel_acknowledged: false,
        snapshot,
        cli_exit_code: exit,
        error: None,
    }
}
fn unknown(target: Target, snapshot: Option<Snapshot>, acknowledged: bool) -> Report {
    let mut report = report(target, snapshot, "cancel_requested", 77);
    report.cancel_requested = true;
    report.cancel_acknowledged = acknowledged;
    report.error = Some(ApiError::new(
        ErrorCode::CommunicationUnknown,
        "cancellation stopping is unconfirmed; retain the IDs and reconnect",
    ));
    report
}
pub async fn cancel(spec: &super::bootstrap::Spec, target: Target) -> Result<Snapshot, ApiError> {
    super::bootstrap::ensure(spec.clone())
        .await
        .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e.to_string()))?;
    let expected = match read(spec, target.clone()).await {
        Ok(snapshot) => snapshot.control_revision,
        Err(error) if error.code == ErrorCode::NotFound && matches!(target, Target::Run(_)) => 0,
        Err(error) => return Err(error),
    };
    let request_id = RunId::generate().to_string();
    cancel_revision(spec, target, expected, &request_id).await
}
async fn cancel_revision(
    spec: &super::bootstrap::Spec,
    target: Target,
    expected_control_revision: u64,
    request_id: &str,
) -> Result<Snapshot, ApiError> {
    let client = super::bootstrap::ensure(spec.clone())
        .await
        .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e.to_string()))?;
    match client
        .request(Operation::Cancel {
            target,
            expected_control_revision,
            request_id: request_id.to_owned(),
        })
        .await
        .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e.to_string()))?
    {
        Reply::Cancelled { snapshot } => Ok(snapshot),
        Reply::ControlError { error } => Err(error),
        _ => Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "unexpected cancellation response",
        )),
    }
}
async fn read(spec: &super::bootstrap::Spec, target: Target) -> Result<Snapshot, ApiError> {
    let reader = super::query::Reader::open(spec.clone())
        .await
        .map_err(|e| ApiError::new(e.code, e.message))?;
    match reader
        .query(Query::Completion { target })
        .await
        .map_err(|e| ApiError::new(e.code, e.message))?
    {
        Response::Completion { snapshot } => Ok(snapshot),
        _ => Err(ApiError::new(
            ErrorCode::Internal,
            "unexpected completion response",
        )),
    }
}
/// Wait never starts an owner. Reopening validates cluster/key/path after restart.
pub async fn wait(
    spec: &super::bootstrap::Spec,
    target: Target,
    signals: &mut mpsc::UnboundedReceiver<()>,
) -> Result<Report, ApiError> {
    let mut last = None;
    loop {
        let status = tokio::select! {biased; _=interrupted(signals)=> {
            return Ok(report(target,last,"wait_interrupted",130));
        },status=read(spec,target.clone())=>status};
        match status {
            Ok(snapshot) => {
                if let Some(exit) = snapshot.exit_code() {
                    return Ok(report(target, Some(snapshot), "settled", exit));
                }
                last = Some(snapshot);
            }
            Err(error)
                if matches!(
                    error.code,
                    ErrorCode::CommunicationUnknown | ErrorCode::Overloaded
                ) => {}
            Err(error) => return Err(error),
        }
        tokio::select! {biased; _=interrupted(signals)=>return Ok(report(target,last,"wait_interrupted",130)),_=tokio::time::sleep(Duration::from_millis(200))=>{}}
    }
}
/// First SIGINT requests durable cancellation; another interrupt only detaches.
pub async fn cancel_and_stop(
    spec: &super::bootstrap::Spec,
    target: Target,
    signals: &mut mpsc::UnboundedReceiver<()>,
    budget: Duration,
) -> Report {
    let deadline = tokio::time::Instant::now() + budget;
    let mut acknowledged = false;
    // exec owns a newly generated run ID; retain the initial revision across retries.
    let expected_control_revision = 0;
    let request_id = RunId::generate().to_string();
    let mut acknowledged_revision = None;
    let mut last = None;
    loop {
        let next = async {
            if !acknowledged {
                cancel_revision(spec, target.clone(), expected_control_revision, &request_id).await
            } else {
                read(spec, target.clone()).await
            }
        };
        let status = tokio::select! {biased; _=interrupted(signals)=>return unknown(target,last,acknowledged),_=tokio::time::sleep_until(deadline)=>return unknown(target,last,acknowledged),status=next=>status};
        if let Ok(snapshot) = status {
            if acknowledged_revision.is_some_and(|revision| snapshot.control_revision != revision) {
                return unknown(target, last, acknowledged);
            }
            acknowledged_revision = Some(snapshot.control_revision);
            acknowledged = true;
            if snapshot.stop_confirmed() {
                let exit = snapshot.exit_code().unwrap_or_else(|| {
                    snapshot
                        .first_compute_failure
                        .as_ref()
                        .map_or(130, |f| f.exit_code)
                });
                let mut report = report(target, Some(snapshot), "stop_confirmed", exit);
                report.cancel_requested = true;
                report.cancel_acknowledged = true;
                return report;
            }
            last = Some(snapshot);
        }
        tokio::select! {biased; _=interrupted(signals)=>return unknown(target,last,acknowledged),_=tokio::time::sleep_until(deadline)=>return unknown(target,last,acknowledged),_=tokio::time::sleep(Duration::from_millis(200))=>{}}
    }
}
pub async fn exec(
    context: Context,
    command: Vec<String>,
    signals: &mut mpsc::UnboundedReceiver<()>,
) -> Result<Report, ApiError> {
    exec_mode(context, command, false, signals).await
}

pub async fn exec_mode(
    context: Context,
    command: Vec<String>,
    in_place: bool,
    signals: &mut mpsc::UnboundedReceiver<()>,
) -> Result<Report, ApiError> {
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let target = Target::Run(run_id.clone());
    let submitted = super::submit::command_with_mode(
        context.clone(),
        command,
        None,
        run_id,
        task_id.clone(),
        in_place,
    );
    let mut result = tokio::select! {biased;
        _=interrupted(signals)=>Ok(cancel_and_stop(&context.bootstrap,target.clone(),signals,Duration::from_secs(30)).await),
        receipt=submitted=>{
            let receipt=receipt?;for warning in receipt.warnings {eprintln!("warning: {warning}");}
            let observed=wait(&context.bootstrap,target.clone(),signals).await?;
            if observed.state=="wait_interrupted" {Ok::<_,ApiError>(cancel_and_stop(&context.bootstrap,target,signals,Duration::from_secs(30)).await)} else {Ok(observed)}
        }
    }?;
    result.task_id = Some(task_id);
    Ok(result)
}

pub async fn retry(spec: &super::bootstrap::Spec, target: Target) -> Result<Report, ApiError> {
    let expected_control_revision = read(spec, target.clone()).await?.control_revision;
    let request_id = RunId::generate().to_string();
    let client = super::bootstrap::ensure(spec.clone())
        .await
        .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e.to_string()))?;
    let operation = Operation::Retry {
        target: target.clone(),
        expected_control_revision,
        request_id,
    };
    match client
        .request(operation)
        .await
        .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e.to_string()))?
    {
        Reply::RetryAccepted { receipt } => {
            let mut result = report(target, None, "queued", 0);
            result.retry_receipt = Some(receipt);
            Ok(result)
        }
        Reply::ControlError { error } => Err(error),
        _ => Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "unexpected retry response",
        )),
    }
}
