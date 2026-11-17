//! The coordinator's side of an ephemeral worker's identity (spec chapter
//! 15): letting an identity in at `hello`, and `workers forget`.
//!
//! Both run under the actor, so no tick looks at the worker's attempts while
//! they are ended, and `forget` also holds the placement fence, so a session
//! cannot be attached between the check that none is and the commit.

use super::identity::{may_forget, Admission, ForgetRefusal};
use super::{Coordinator, Result};
use crate::ids::{WorkerBootId, WorkerId};
use crate::platform::wall_ms;
use crate::storage::Forgotten;

impl Coordinator {
    /// Read the currently attached names and the durable ephemeral identity
    /// without dialing or renewing any worker.
    pub(crate) async fn worker_observations(
        &self,
        names: Vec<WorkerId>,
    ) -> Result<std::collections::BTreeMap<WorkerId, (bool, Option<String>)>> {
        let _actor = self.actor.lock().await;
        let _commit = self.placement_fence.lock().await;
        let attached: std::collections::BTreeSet<_> = self
            .workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();
        self.db(move |store| {
            names
                .into_iter()
                .map(|name| {
                    let id = store.worker_identity(&name)?;
                    let connected = attached.contains(&name);
                    Ok((name, (connected, id)))
                })
                .collect()
        })
        .await
    }

    /// An ephemeral worker reported `boot` at `hello`. A retired identity is
    /// refused (`Admission::Retired`) and must not be attached. A different identity must wait for explicit forget.
    pub async fn admit_identity(
        &self,
        worker: &WorkerId,
        boot: &WorkerBootId,
    ) -> Result<Admission> {
        let _actor = self.actor.lock().await;
        let (w, b) = (worker.clone(), boot.clone());
        self.db(move |s| s.admit_worker_identity(&w, &b, wall_ms()))
            .await
    }

    /// Publish the link under the same fence that protects forget. A handshake
    /// accepted before forget must never expose its link after the tombstone.
    pub(crate) async fn attach_ephemeral(
        &self,
        worker: WorkerId,
        boot: &WorkerBootId,
        link: std::sync::Arc<dyn super::WorkerLink>,
    ) -> Result<Admission> {
        let _actor = self.actor.lock().await;
        let _commit = self.placement_fence.lock().await;
        let (name, identity) = (worker.clone(), boot.clone());
        let admission = self
            .db(move |s| s.admit_worker_identity(&name, &identity, wall_ms()))
            .await?;
        if matches!(admission, Admission::First | Admission::Same) {
            self.workers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(worker, link);
        }
        Ok(admission)
    }

    /// Whether `boot` was retired for `worker`. Asked again after a session
    /// was attached: a `forget` that ran between the admission and the
    /// attachment is seen here.
    pub async fn identity_retired(&self, worker: &WorkerId, boot: &WorkerBootId) -> Result<bool> {
        let (w, b) = (worker.clone(), boot.clone());
        self.db(move |s| s.worker_identity_retired(&w, &b)).await
    }

    /// `workers forget`: the user declared an ephemeral worker gone.
    /// `ephemeral` is what its registration says (`None`: not registered).
    /// Refused for a persistent worker and for one that is connected.
    /// Otherwise its identity is retired and every attempt it still holds
    /// ends as `interrupted`, in one commit.
    pub async fn forget_worker(
        &self,
        worker: &WorkerId,
        ephemeral: Option<bool>,
    ) -> Result<std::result::Result<Forgotten, ForgetRefusal>> {
        let _actor = self.actor.lock().await;
        let _commit = self.placement_fence.lock().await;
        let connected = self
            .workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(worker);
        if let Err(refusal) = may_forget(ephemeral, connected) {
            return Ok(Err(refusal));
        }
        let w = worker.clone();
        Ok(Ok(self.db(move |s| s.forget_worker(&w, wall_ms())).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::super::link::LinkFuture;
    use super::super::tests::{fx, Fx};
    use super::super::*;
    use super::*;
    use crate::ids::Digest;
    use crate::service::completion::Target;
    use std::sync::atomic::AtomicUsize;

    /// A worker that started clean: it knows no attempt, and counts the
    /// submits it is sent.
    #[derive(Default)]
    struct Clean {
        submits: AtomicUsize,
    }
    impl WorkerLink for Clean {
        fn call(&self, request: Request) -> LinkFuture<'_> {
            let submit = matches!(request, Request::SubmitAttempt(_));
            let asked = matches!(request, Request::GetAttempt(_));
            if submit {
                self.submits.fetch_add(1, Ordering::SeqCst);
            }
            Box::pin(async move {
                if asked {
                    Err(LinkError::Remote(ApiError::new(
                        ErrorCode::NotFound,
                        "no such attempt",
                    )))
                } else {
                    Err(LinkError::Unconfirmed)
                }
            })
        }
    }

    fn boot(text: &str) -> WorkerBootId {
        WorkerBootId::parse(text).unwrap()
    }

    fn attempt_of(f: &Fx, attempt: &AttemptId) -> AttemptRecord {
        f.c.store().get_attempt(attempt).unwrap().unwrap()
    }

    /// A46, the parent's half: an attempt on an ephemeral worker that
    /// became unreachable stays `unknown` and is given to nobody else;
    /// only `forget` ends it as `interrupted`, retires the identity, and
    /// the retired identity is refused afterwards while a new one is let in.
    #[tokio::test]
    async fn an_unreachable_ephemeral_worker_keeps_its_attempt_unknown_until_it_is_forgotten() {
        let f = fx();
        let first = boot("os-boot.start-1");
        assert_eq!(
            f.c.admit_identity(&f.worker, &first).await.unwrap(),
            Admission::First
        );
        let link: Arc<dyn WorkerLink> = Arc::new(Clean::default());
        f.c.attach(f.worker.clone(), link.clone()).await;
        let (task, attempt) = f.placed().await;

        let observations =
            f.c.worker_observations(vec![f.worker.clone()])
                .await
                .unwrap();
        assert_eq!(observations[&f.worker], (true, Some(first.to_string())));

        // Connected: forget is refused and changes nothing.
        assert_eq!(
            f.c.forget_worker(&f.worker, Some(true)).await.unwrap(),
            Err(ForgetRefusal::Connected)
        );
        assert_eq!(
            f.c.forget_worker(&f.worker, Some(false)).await.unwrap(),
            Err(ForgetRefusal::NotEphemeral)
        );
        assert_eq!(
            f.c.forget_worker(&f.worker, None).await.unwrap(),
            Err(ForgetRefusal::NotRegistered)
        );
        assert_ne!(attempt_of(&f, &attempt).state, AttemptState::Terminal);

        // The tunnel is gone. Another worker has room the whole time.
        assert!(f.c.detach_link(&f.worker, &link).await.unwrap());
        let observations =
            f.c.worker_observations(vec![f.worker.clone()])
                .await
                .unwrap();
        assert_eq!(observations[&f.worker], (false, Some(first.to_string())));
        let other = WorkerId::parse("w-b").unwrap();
        let spare = Arc::new(Clean::default());
        f.c.attach(other.clone(), spare.clone()).await;
        for _ in 0..3 {
            f.c.tick().await.unwrap();
            let kept = attempt_of(&f, &attempt);
            assert_ne!(kept.state, AttemptState::Terminal);
            assert_eq!(kept.observation, Observation::Unknown);
            assert!(!kept.retired);
        }
        assert_eq!(f.c.store().attempts_of_task(&task).unwrap().len(), 1);
        assert_eq!(spare.submits.load(Ordering::SeqCst), 0);
        assert!(!f.c.identity_retired(&f.worker, &first).await.unwrap());

        // A different boot cannot claim this name or settle the old attempt.
        let second = boot("os-boot.start-2");
        assert_eq!(
            f.c.admit_identity(&f.worker, &second).await.unwrap(),
            Admission::Conflict
        );
        assert_eq!(
            f.c.admit_identity(&f.worker, &first).await.unwrap(),
            Admission::Same
        );
        f.c.tick().await.unwrap();
        let kept = attempt_of(&f, &attempt);
        assert_eq!(kept.observation, Observation::Unknown);
        assert!(kept.termination.is_none());

        let target = Target::Task(task.clone());
        let revision =
            f.c.store()
                .completion(target.clone())
                .unwrap()
                .control_revision;
        let request = "retry-after-forget".to_string();
        let refused =
            f.c.store()
                .retry(target.clone(), revision, request.clone(), 1, |_, _, _| {
                    panic!("an unknown attempt cannot reach retry validation")
                })
                .unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("retry requires known terminal stopping"),
            "{refused}"
        );

        // The user declares it gone.
        let forgotten =
            f.c.forget_worker(&f.worker, Some(true))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(forgotten.identity.as_deref(), Some(first.as_str()));
        assert_eq!(forgotten.ended, vec![attempt.clone()]);
        let ended = attempt_of(&f, &attempt);
        assert_eq!(ended.state, AttemptState::Terminal);
        assert_eq!(ended.observation, Observation::Known);
        assert_eq!(
            ended.termination.map(|t| t.outcome()),
            Some(Outcome::Interrupted)
        );
        // Nothing is left waiting: the task is computed and its collection
        // is closed, since nothing can be fetched from a worker that is gone.
        let snapshot = f.c.store().completion(Target::Task(task.clone())).unwrap();
        assert_eq!(snapshot.computed_tasks, 1);
        assert_eq!(snapshot.uncertain_tasks, 0);
        assert_eq!(snapshot.collection_pending_tasks, 0);
        let events =
            f.c.store()
                .events_of(&attempt, "worker_identity_retired")
                .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload["reason"], "forgotten");

        // Ended, not placed again by itself: a retry is the user's to ask for.
        f.c.tick().await.unwrap();
        assert_eq!(f.c.store().attempts_of_task(&task).unwrap().len(), 1);
        assert_eq!(spare.submits.load(Ordering::SeqCst), 0);

        // The old identity comes back late: refused, however often.
        assert!(f.c.identity_retired(&f.worker, &first).await.unwrap());
        for _ in 0..2 {
            assert_eq!(
                f.c.admit_identity(&f.worker, &first).await.unwrap(),
                Admission::Retired
            );
        }
        assert_eq!(f.c.store().worker_identity(&f.worker).unwrap(), None);
        // The waiting identity can now use the name.
        assert_eq!(
            f.c.admit_identity(&f.worker, &second).await.unwrap(),
            Admission::First
        );
        assert_eq!(
            f.c.admit_identity(&f.worker, &second).await.unwrap(),
            Admission::Same
        );
        // Forgetting again retires that one too and ends nothing more.
        let again =
            f.c.forget_worker(&f.worker, Some(true))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(again.identity.as_deref(), Some(second.as_str()));
        assert!(again.ended.is_empty());
        let nothing =
            f.c.forget_worker(&f.worker, Some(true))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(nothing.identity, None);

        let receipt =
            f.c.store()
                .retry(
                    target.clone(),
                    snapshot.control_revision,
                    request.clone(),
                    2,
                    |_, _, digest| {
                        assert_eq!(digest, &Digest::of_bytes(b"manifest"));
                        Ok(false)
                    },
                )
                .unwrap();
        let replay =
            f.c.store()
                .retry(target, snapshot.control_revision, request, 3, |_, _, _| {
                    panic!("replaying a committed retry must reuse its receipt")
                })
                .unwrap();
        assert_eq!(receipt.task_count, 1);
        assert_eq!(receipt.task_id.as_ref(), Some(&task));
        assert_eq!(receipt.control_revision, snapshot.control_revision + 1);
        assert_eq!(receipt.control_revision, replay.control_revision);
        assert_eq!(receipt.min_generation, replay.min_generation);
        assert_eq!(
            attempt_of(&f, &attempt).termination.unwrap().outcome(),
            Outcome::Interrupted
        );
    }

    /// A cancel that was asked for before the worker vanished is what the
    /// attempt ends as.
    #[tokio::test]
    async fn a_cancelled_attempt_of_a_forgotten_worker_ends_cancelled() {
        let f = fx();
        f.c.admit_identity(&f.worker, &boot("b.1")).await.unwrap();
        let (task, attempt) = f.placed().await;
        f.c.cancel_task(&task).await.unwrap();
        let forgotten =
            f.c.forget_worker(&f.worker, Some(true))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(forgotten.ended, vec![attempt.clone()]);
        assert_eq!(
            attempt_of(&f, &attempt).termination.map(|t| t.outcome()),
            Some(Outcome::Cancelled)
        );
    }
}
