//! The parent's side of stage 3 (spec 25.6 (a)): `release_attempt` for the
//! attempts it reads no more of, the reconciliation at each connection
//! (`list_unreleased`), and the rule that no acknowledgement or release of
//! an attempt is sent while a submit of it waits for its answer.

use super::{Coordinator, Result, WorkerLink};
use crate::ids::{AttemptId, RequestId, WorkerId};
use crate::results::plan::Verdict;
use crate::wire::{ListUnreleasedRequest, ReleaseAttemptRequest, Request, Response, SchemaVersion};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

/// Pages of `list_unreleased` one reconciliation reads; a worker with more
/// stays due and the next pass goes on.
const PAGES: usize = 8;

/// The attempts whose submit is on its way: neither `ack_collected` nor
/// `release_attempt` is sent for them until it is answered (or its
/// connection failed).
#[derive(Debug, Default)]
pub struct InFlight(Mutex<BTreeSet<AttemptId>>);

/// A submit in flight; it leaves the set when dropped.
pub struct Sending<'a>(&'a InFlight, AttemptId);

impl Drop for Sending<'_> {
    fn drop(&mut self) {
        self.0.set().remove(&self.1);
    }
}

impl InFlight {
    fn set(&self) -> std::sync::MutexGuard<'_, BTreeSet<AttemptId>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A submit of `attempt` is sent from now until the guard goes.
    pub fn sending(&self, attempt: &AttemptId) -> Sending<'_> {
        self.set().insert(attempt.clone());
        Sending(self, attempt.clone())
    }

    pub fn contains(&self, attempt: &AttemptId) -> bool {
        self.set().contains(attempt)
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

/// Send `release_attempt` for each of `attempts` over `link`, unless a
/// submit of it is in flight or the link did not agree the feature. Every
/// answer is accepted as it comes: a worker that has nothing of the attempt
/// any more (`NOT_FOUND`) has what the release asks for, and one that does
/// not answer lists it again at the next connection.
pub async fn release_over(
    link: &dyn WorkerLink,
    submits: &InFlight,
    attempts: &[AttemptId],
) -> bool {
    if !link.attempt_release_negotiated() {
        return true;
    }
    for attempt in attempts {
        if submits.contains(attempt) {
            continue;
        }
        let answer = link
            .call(Request::ReleaseAttempt(ReleaseAttemptRequest {
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(),
                attempt_id: attempt.clone(),
            }))
            .await;
        // A worker that does not answer is not asked the rest now: what is
        // left is listed again at the next reconciliation.
        if matches!(answer, Err(crate::coordinator::LinkError::Unconfirmed)) {
            return false;
        }
    }
    true
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
            if !release_over(link.as_ref(), &self.submits, attempts).await {
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
        for worker in due {
            let Some(link) = self.link_of(&worker) else {
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
            if !self.reconcile_one(&link).await? {
                self.reconcile_due(&worker);
            }
        }
        Ok(())
    }

    /// One worker; `true` when every page was read.
    async fn reconcile_one(&self, link: &dyn WorkerLink) -> Result<bool> {
        let mut after = None;
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
            if !release_over(link, &self.submits, &judged).await {
                return Ok(false);
            }
            if !page.more {
                return Ok(true);
            }
            after = page.attempts.last().cloned();
            if after.is_none() {
                return Ok(true);
            }
        }
        Ok(false)
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
        }
        assert!(!f.contains(&a));
    }
}
