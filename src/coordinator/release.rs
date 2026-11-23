//! The parent's side of stage 3 (spec 25.6 (a)): `release_attempt` for the
//! attempts it reads no more of, the reconciliation at each connection
//! (`list_unreleased`), and the rule that no acknowledgement or release of
//! an attempt is sent while a submit of it waits for its answer.

use super::{Coordinator, Result, WorkerLink};
use crate::ids::{AttemptId, RequestId, WorkerId};
use crate::results::plan::Verdict;
use crate::wire::{ListUnreleasedRequest, ReleaseAttemptRequest, Request, Response, SchemaVersion};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Pages of `list_unreleased` one reconciliation reads; a worker with more
/// stays due and the next pass goes on.
const PAGES: usize = 8;

/// The attempts whose submit is on its way: neither `ack_collected` nor
/// `release_attempt` is sent for them until it is answered (or its
/// connection failed). Counted: two submits of one attempt may be on their
/// way at once, and the first answer must not clear the mark of the other.
#[derive(Debug, Default)]
pub struct InFlight(Mutex<BTreeMap<AttemptId, usize>>);

/// A submit in flight; it is uncounted when dropped.
pub struct Sending<'a>(&'a InFlight, AttemptId);

impl Drop for Sending<'_> {
    fn drop(&mut self) {
        let mut set = self.0.set();
        if let Some(n) = set.get_mut(&self.1) {
            *n -= 1;
            if *n == 0 {
                set.remove(&self.1);
            }
        }
    }
}

impl InFlight {
    fn set(&self) -> std::sync::MutexGuard<'_, BTreeMap<AttemptId, usize>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A submit of `attempt` is on its way from now until the guard goes.
    pub fn sending(&self, attempt: &AttemptId) -> Sending<'_> {
        *self.set().entry(attempt.clone()).or_insert(0) += 1;
        Sending(self, attempt.clone())
    }

    pub fn contains(&self, attempt: &AttemptId) -> bool {
        self.set().contains_key(attempt)
    }
}

/// What the parent does with an attempt a worker listed as unreleased.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unreleased {
    /// No row (pruned), or a collection that gave up: release it.
    Release,
    /// The ordinary collection takes care of it.
    Keep,
}

/// The decision of the reconciliation for one listed attempt (spec 25.6
/// (a)): no row says this parent never sends the attempt again; a
/// collection closed by giving up reads nothing more of it.
pub fn judge_unreleased(row: bool, verdict: Option<Verdict>) -> Unreleased {
    match (row, verdict) {
        (false, _) => Unreleased::Release,
        (true, Some(Verdict::GaveUp)) => Unreleased::Release,
        (true, _) => Unreleased::Keep,
    }
}

/// What [`release_over`] got done.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Released {
    /// Every attempt is released (or the link does not have the feature).
    All,
    /// The rest went, but a submit of one was in flight: it is sent again.
    Later,
    /// The worker refused `.0` for now (`OVERLOADED`: a supervisor still
    /// holds the attempt's lock, a trial); the ones after it were not sent,
    /// since the same refusal would answer them.
    Refused(AttemptId),
    /// The worker did not answer; the rest is not asked now.
    Lost,
}

/// Send `release_attempt` for each of `attempts` over `link`, unless a
/// submit of it is in flight or the link did not agree the feature. A
/// worker that has nothing of the attempt any more (`NOT_FOUND`) has what
/// the release asks for. Anything but [`Released::All`] keeps the worker due.
pub async fn release_over(
    link: &dyn WorkerLink,
    submits: &InFlight,
    attempts: &[AttemptId],
) -> Released {
    if !link.attempt_release_negotiated() {
        return Released::All;
    }
    let mut all = Released::All;
    for attempt in attempts {
        if submits.contains(attempt) {
            all = Released::Later;
            continue;
        }
        let answer = link
            .call(Request::ReleaseAttempt(ReleaseAttemptRequest {
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(),
                attempt_id: attempt.clone(),
            }))
            .await;
        match answer {
            Err(crate::coordinator::LinkError::Unconfirmed) => return Released::Lost,
            Err(crate::coordinator::LinkError::Remote(e))
                if e.code == crate::error::ErrorCode::Overloaded =>
            {
                return Released::Refused(attempt.clone());
            }
            _ => {}
        }
    }
    all
}

impl Coordinator {
    fn link_of(&self, worker: &WorkerId) -> Option<Arc<dyn WorkerLink>> {
        self.workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(worker)
            .cloned()
    }

    /// The submits on their way (shared with the result collection).
    pub(crate) fn submits_in_flight(&self) -> Arc<InFlight> {
        Arc::clone(&self.submits)
    }

    /// Release `attempts` on `worker` now if it is attached (spec 25.5 step
    /// 7). One that is not attached lists them at its next connection.
    pub async fn release_on(&self, worker: &WorkerId, attempts: &[AttemptId]) {
        if let Some(link) = self.link_of(worker) {
            if release_over(link.as_ref(), &self.submits, attempts).await != Released::All {
                self.reconcile_due(worker);
            }
        }
    }

    /// The reconciliation is due for `worker`: at each connection, and when
    /// the worker refused a submit for its bound of records.
    pub(crate) fn reconcile_due(&self, worker: &WorkerId) {
        self.unreleased_due
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(worker.clone());
    }

    /// Reconcile every attached worker that is due (spec 25.6 (a), "接続時の
    /// 照合"): page through what it holds of this parent that is terminal
    /// and neither acknowledged nor released, and release what has no row
    /// here or whose collection gave up. A worker that does not answer, or
    /// has more than [`PAGES`] pages, stays due.
    pub(crate) async fn reconcile_unreleased(&self, deadline: tokio::time::Instant) -> Result<()> {
        let due: Vec<WorkerId> = std::mem::take(
            &mut *self
                .unreleased_due
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        )
        .into_iter()
        .collect();
        for (i, worker) in due.iter().enumerate() {
            let Some(link) = self.link_of(worker) else {
                // Not attached: the next attach makes it due again.
                continue;
            };
            if !link.attempt_release_negotiated() {
                continue;
            }
            let link = super::link::Budgeted {
                inner: link,
                deadline,
            };
            match self.reconcile_one(worker, &link).await {
                Ok(true) => {}
                Ok(false) => self.reconcile_due(worker),
                Err(e) => {
                    // This one and the ones not reached stay due.
                    for w in &due[i..] {
                        self.reconcile_due(w);
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn cursor(&self) -> std::sync::MutexGuard<'_, std::collections::BTreeMap<WorkerId, AttemptId>> {
        self.unreleased_after
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// One worker, from where its last pass stopped; `true` when the pass
    /// read the whole list from the front and everything went. A pass that
    /// began at a cursor and reached the end wraps and stays due, so the
    /// front is read by the next pass. A page the worker did not answer is
    /// read again; a refusal (`OVERLOADED`) moves the cursor past the
    /// refused attempt, so the pages after it are reached, and the wrap
    /// comes back to it.
    async fn reconcile_one(&self, worker: &WorkerId, link: &dyn WorkerLink) -> Result<bool> {
        let mut after = self.cursor().get(worker).cloned();
        let mut whole = after.is_none();
        for _ in 0..PAGES {
            let answer = link
                .call(Request::ListUnreleased(ListUnreleasedRequest {
                    schema_version: SchemaVersion::CURRENT,
                    request_id: RequestId::generate(),
                    after: after.clone(),
                    limit: crate::storage::MAX_UNRELEASED_PAGE,
                }))
                .await;
            let Ok(Response::ListUnreleased(page)) = answer else {
                self.keep_cursor(worker, after);
                return Ok(false);
            };
            let listed = page.attempts.clone();
            let judged = self
                .db(move |s| {
                    let mut out = Vec::new();
                    for a in listed {
                        let row = s.get_attempt(&a)?.is_some();
                        let verdict = if row {
                            s.collection(&a)?.and_then(|c| c.verdict)
                        } else {
                            None
                        };
                        if judge_unreleased(row, verdict) == Unreleased::Release {
                            out.push(a);
                        }
                    }
                    Ok(out)
                })
                .await?;
            match release_over(link, &self.submits, &judged).await {
                Released::All => {}
                Released::Later => whole = false,
                Released::Refused(at) => {
                    self.keep_cursor(worker, Some(at));
                    return Ok(false);
                }
                Released::Lost => {
                    self.keep_cursor(worker, after);
                    return Ok(false);
                }
            }
            let next = page.attempts.last().cloned();
            if !page.more || next.is_none() {
                self.cursor().remove(worker);
                return Ok(whole);
            }
            after = next;
        }
        self.keep_cursor(worker, after);
        Ok(false)
    }

    fn keep_cursor(&self, worker: &WorkerId, after: Option<AttemptId>) {
        match after {
            Some(a) => {
                self.cursor().insert(worker.clone(), a);
            }
            None => {
                self.cursor().remove(worker);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_missing_row_or_a_collection_that_gave_up_is_released() {
        assert_eq!(judge_unreleased(false, None), Unreleased::Release);
        assert_eq!(
            judge_unreleased(false, Some(Verdict::Complete)),
            Unreleased::Release
        );
        assert_eq!(
            judge_unreleased(true, Some(Verdict::GaveUp)),
            Unreleased::Release
        );
        for v in [
            None,
            Some(Verdict::Complete),
            Some(Verdict::RequiredMissing),
        ] {
            assert_eq!(judge_unreleased(true, v), Unreleased::Keep, "{v:?}");
        }
    }

    #[test]
    fn a_submit_in_flight_holds_the_release_until_it_is_answered() {
        let f = InFlight::default();
        let a = AttemptId::generate();
        {
            let _s = f.sending(&a);
            assert!(f.contains(&a));
            // A second submit of the same attempt: the first answer does
            // not clear the mark of the other.
            let second = f.sending(&a);
            drop(_s);
            assert!(f.contains(&a));
            drop(second);
        }
        assert!(!f.contains(&a));
    }
}
