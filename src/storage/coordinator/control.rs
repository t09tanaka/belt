//! Durable run cancellation may precede enqueue. Reads expose metadata only.
use super::*;
use crate::service::completion::{Snapshot, Target};
impl CoordinatorStore {
    pub fn cancel_run(&self, run: &RunId, at_ms: u64) -> Result<()> {
        let run = run.to_string();
        self.db.write(move |tx| {
            let fresh=tx.execute("INSERT OR IGNORE INTO run_cancel_intent(run_id,requested_at_ms) VALUES (?1,?2)",params![run,to_i64(at_ms)?])?;
            tx.execute("UPDATE task SET cancel_requested_at_ms=COALESCE(cancel_requested_at_ms,?1) WHERE run_id=?2",params![to_i64(at_ms)?,run])?;
            tx.execute("INSERT INTO event(run_id,attempt_id,attempt_seq,kind,payload_json,at_ms) SELECT ?1,a.attempt_id,(SELECT COALESCE(MAX(e.attempt_seq),0)+1 FROM event e WHERE e.attempt_id=a.attempt_id),'cancel_requested','null',?2 FROM attempt a WHERE a.task_id IN (SELECT task_id FROM task WHERE run_id=?1) AND a.state<>'terminal' AND a.retired=0 AND a.cancel_requested_at_ms IS NULL",params![run,to_i64(at_ms)?])?;
            tx.execute("UPDATE attempt SET cancel_requested_at_ms=?1 WHERE task_id IN (SELECT task_id FROM task WHERE run_id=?2) AND state<>'terminal' AND retired=0 AND cancel_requested_at_ms IS NULL",params![to_i64(at_ms)?,run])?;
            if fresh==1 {common::append_event(tx,Some(&run),None,"run_cancel_requested",&Value::Null,at_ms)?;}
            Ok(())
        })
    }
    pub fn completion(&self, target: Target) -> Result<Snapshot> {
        let collections = self.collections;
        let task_cancel = self.task_cancel;
        self.db.call(move |c| {
            let tx=c.unchecked_transaction()?;
            let has_intents:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='run_cancel_intent')",[],|r|r.get(0))?;
            let (run,task)=match &target {Target::Run(id)=>(id.to_string(),None),Target::Task(id)=>{
                let run:Option<String>=tx.query_row("SELECT run_id FROM task WHERE task_id=?1",[id.to_string()],|r|r.get(0)).optional()?;
                (run.ok_or_else(||not_found(format!("task {id}")))?,Some(id.to_string()))
            }};
            let accepted:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM run WHERE run_id=?1)",[&run],|r|r.get(0))?;
            let cancelled=if has_intents {tx.query_row("SELECT EXISTS(SELECT 1 FROM run_cancel_intent WHERE run_id=?1)",[&run],|r|r.get::<_,bool>(0))?} else {false};
            if !accepted&&!cancelled {return Err(not_found(format!("run {run}")));}
            let mut snapshot=Snapshot::new(target,accepted,cancelled);
            let cancel=if task_cancel {"t.cancel_requested_at_ms IS NOT NULL"}else{"0"};
            let (join,verdict)=if collections {("LEFT JOIN collection x ON x.attempt_id=a.attempt_id","CASE WHEN x.closed_at_ms IS NOT NULL THEN x.verdict ELSE NULL END")}else{("","NULL")};
            let sql=format!("SELECT t.task_id,{cancel},a.state,a.observation,a.termination_json,{verdict} FROM task t LEFT JOIN attempt a ON a.attempt_id=(SELECT aa.attempt_id FROM attempt aa WHERE aa.task_id=t.task_id AND aa.retired=0 ORDER BY aa.generation DESC LIMIT 1) {join} WHERE t.run_id=?1 AND (?2 IS NULL OR t.task_id=?2) ORDER BY t.task_id");
            let mut stmt=tx.prepare(&sql)?;let mut rows=stmt.query(params![run,task])?;
            while let Some(row)=rows.next()? {
                let id=TaskId::parse(&row.get::<_,String>(0)?).map_err(|e|StorageError::Corrupt(e.to_string()))?;
                let cancel:bool=row.get(1)?;
                let state:Option<String>=row.get(2)?;let observation:Option<String>=row.get(3)?;let termination:Option<String>=row.get(4)?;let collection:Option<String>=row.get(5)?;
                let state=state.map(|v|serde_json::from_value(serde_json::json!(v)).map_err(|e|StorageError::Corrupt(e.to_string()))).transpose()?;
                let observation=observation.map(|v|serde_json::from_value(serde_json::json!(v)).map_err(|e|StorageError::Corrupt(e.to_string()))).transpose()?;
                let termination=termination.map(|v|from_json("termination",&v)).transpose()?;
                if collection.as_deref().is_some_and(|v|!matches!(v,"complete"|"required_missing"|"gave_up")) {return Err(StorageError::Corrupt("invalid collection verdict".into()));}
                snapshot.cancel_requested|=cancel;
                snapshot.task(id,state,observation,termination,cancel,collection.as_deref());
            }
            Ok(snapshot)
        })
    }
}
