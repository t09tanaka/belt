//! Independent, bounded delivery jobs for one verified worker connection.
//! A future connection owner can use these jobs without routing source IO through ticks.
use super::*;
use crate::coordinator::InputGate;

pub(super) async fn sources(
    coordinator: Arc<Coordinator>,
    capture: Arc<CapturedInputs>,
    worker: crate::protocol::Client,
    worker_id: WorkerId,
    gate: Arc<InputGate>,
    mut input_stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut queued_cursor = None;
    let mut live_cursor = None;
    let mut timer = tokio::time::interval(Duration::from_millis(100));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { _=input_stop.changed()=>return Ok(()), _=timer.tick()=>{} }
        let owner = coordinator.clone();
        let id = worker_id.clone();
        let snapshot = gate.placement_inputs();
        let generation = snapshot.generation;
        let ready = snapshot.ready;
        let needed =
            tokio::task::spawn_blocking(move || -> crate::storage::Result<Vec<(RunId, bool)>> {
                let mut needed = Vec::new();
                for input in ready {
                    let (queued, live) = owner.store().input_need(&input.run_id, &id)?;
                    if queued || live {
                        needed.push((input.run_id, queued));
                    }
                }
                Ok(needed)
            })
            .await??;
        gate.retain(|i| needed.iter().any(|(run, _)| run == &i.run_id));
        for (run, queued) in &needed {
            if !queued {
                gate.evaluated(generation, run);
            }
        }
        if gate.snapshot().len() >= 64 && !gate.evict_evaluated() {
            continue;
        }
        let owner = coordinator.clone();
        let id = worker_id.clone();
        let (live, queued) = tokio::task::spawn_blocking(move || -> crate::storage::Result<_> {
            Ok((
                owner.store().input_page(live_cursor, 64, Some(&id))?,
                owner.store().input_page(queued_cursor, 64, None)?,
            ))
        })
        .await??;
        live_cursor = live.last().map(|(cursor, _)| *cursor);
        queued_cursor = queued.last().map(|(cursor, _)| *cursor);
        let mut discovered = std::collections::BTreeSet::new();
        for (_, task) in live.into_iter().chain(queued) {
            if !discovered.insert(task.run_id.clone()) {
                continue;
            }
            if gate.snapshot().len() >= 64 {
                break;
            }
            let Some(source) = task.source_id else {
                continue;
            };
            let Ok(spec) = serde_json::from_value::<TaskSpec>(task.spec) else {
                continue;
            };
            let generation = gate.generation();
            let input = crate::coordinator::ReadyInput {
                run_id: task.run_id,
                source_id: source.clone(),
                manifest_digest: spec.manifest_digest,
            };
            if gate.snapshot().contains(&input) {
                continue;
            }
            // Verify first so legacy submissions with preseeded worker inputs
            // recover after restart without requiring a parent capture manifest.
            let verified = super::super::forward::verify(&worker, &source, &mut input_stop)
                .await
                .is_ok_and(|v| v.ready);
            let delivered = if verified && !spec.has_secret_bundle {
                true
            } else {
                let captured = capture.clone();
                let expected = input.clone();
                let secrets = spec.has_secret_bundle;
                let validation = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                    let id = crate::source::classify::ci_id_of(&expected.source_id)
                        .ok_or_else(|| anyhow::anyhow!("invalid source"))?;
                    anyhow::ensure!(
                        captured.sources.source_of_run(&expected.run_id)?.as_deref() == Some(&id),
                        "run source binding mismatch"
                    );
                    let manifest = captured
                        .sources
                        .manifest(&id)?
                        .ok_or_else(|| anyhow::anyhow!("source unavailable"))?;
                    crate::source::store::verify_manifest(&manifest)?;
                    let digest = Digest::parse(&manifest.manifest_digest).or_else(|_| {
                        Digest::parse(&format!("sha256:{}", manifest.manifest_digest))
                    })?;
                    anyhow::ensure!(
                        digest == expected.manifest_digest
                            && secrets == !manifest.secret_files.is_empty(),
                        "source digest mismatch"
                    );
                    captured
                        .vault
                        .load(&expected.run_id, &manifest.secret_files, &captured.key)?;
                    Ok(())
                });
                let valid = tokio::select! { _=input_stop.changed()=>return Ok(()), result=validation=>result? };
                if valid.is_err() {
                    continue;
                }
                if verified {
                    true
                } else {
                    let transfer = tokio::select! { _=input_stop.changed()=>return Ok(()), guard=capture.transfer.clone().lock_owned()=>guard };
                    let store = capture.sources.clone();
                    let env = capture.env.clone();
                    let epoch = coordinator.epoch();
                    let client = worker.clone();
                    let (cancel, stopped) = watch::channel(false);
                    // This detached job owns only captured IO and its lease. Shutdown
                    // releases resident ownership while blocking exporter cleanup drains.
                    let mut job = tokio::spawn(async move {
                        super::super::forward::source(
                            &client, epoch, store, &source, env, stopped, transfer,
                        )
                        .await
                    });
                    tokio::select! {
                        _=input_stop.changed()=> { let _=cancel.send(true); return Ok(()); },
                        _ = tokio::time::sleep(Duration::from_secs(30)) => { let _=cancel.send(true); tokio::select! { _=input_stop.changed()=>return Ok(()), result=job=>result?.is_ok() } },
                        result=&mut job=>result?.is_ok(),
                    }
                }
            };
            if delivered && !*input_stop.borrow() {
                gate.publish(generation, input);
            }
        }
    }
}

pub(super) async fn secrets(
    coordinator: Arc<Coordinator>,
    captured: Arc<CapturedInputs>,
    worker_client: crate::protocol::Client,
    worker_id: WorkerId,
    gate: Arc<InputGate>,
    mut secret_stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut timer = tokio::time::interval(Duration::from_millis(100));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sent = std::collections::BTreeSet::<(u64, AttemptId, Generation)>::new();
    let mut next = None;
    loop {
        tokio::select! { _ = secret_stop.changed() => return Ok(()), _ = timer.tick() => {} }
        let owner = coordinator.clone();
        let worker = worker_id.clone();
        let cached: Vec<_> = sent.iter().map(|(_, attempt, _)| attempt.clone()).collect();
        let (attempts, active) =
            tokio::task::spawn_blocking(move || -> crate::storage::Result<_> {
                Ok((
                    owner.store().live_attempt_page(&worker, next, 16)?,
                    owner.store().live_attempt_subset(&worker, &cached)?,
                ))
            })
            .await??;
        next = attempts.last().map(|(cursor, _)| *cursor);
        let generation = gate.generation();
        sent.retain(|(session, attempt, _)| *session == generation && active.contains(attempt));
        for (_, attempt) in attempts {
            let Some(placement) = attempt.placement else {
                continue;
            };
            let payload: crate::wire::AttemptPayload =
                serde_json::from_value(placement.demand["payload"].clone())?;
            let key = (
                gate.generation(),
                attempt.record.attempt_id.clone(),
                attempt.record.generation,
            );
            if !payload.has_secret_bundle || sent.contains(&key) {
                continue;
            }
            let capture = captured.clone();
            let run = payload.run_id.clone();
            let source = payload.source_id.clone();
            let loading = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                let id = crate::source::classify::ci_id_of(&source)
                    .ok_or_else(|| anyhow::anyhow!("invalid source"))?;
                anyhow::ensure!(
                    capture.sources.source_of_run(&run)?.as_deref() == Some(&id),
                    "run source binding mismatch"
                );
                let manifest = capture
                    .sources
                    .manifest(&id)?
                    .ok_or_else(|| anyhow::anyhow!("input source unavailable"))?;
                Ok(capture
                    .vault
                    .load(&run, &manifest.secret_files, &capture.key)?)
            });
            let bundle =
                tokio::select! { _=secret_stop.changed()=>return Ok(()), bundle=loading=>bundle? };
            // Missing/tampered bundles never become an empty bundle. The worker's
            // secret gate eventually reports SECRET_UNAVAILABLE without running.
            let Ok(bundle) = bundle else {
                continue;
            };
            let delivered = tokio::select! {
                _ = secret_stop.changed() => return Ok(()),
                result = crate::secrets::send::send_bundle(&worker_client, &bundle,
                    coordinator.epoch(), &attempt.record.attempt_id, attempt.record.generation, IO_TIMEOUT) => result.is_ok(),
            };
            if delivered {
                if sent.len() >= 64 {
                    sent.clear();
                }
                sent.insert(key);
            }
        }
    }
}
