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
    /// A persistent member's changed identity means its old work has stopped
    /// after send-key. A reboot alone keeps attempts and collections open.
    pub(crate) async fn attach_identity(
        &self,
        worker: WorkerId,
        boot: &WorkerBootId,
        link: std::sync::Arc<dyn super::WorkerLink>,
        ephemeral: bool,
    ) -> Result<Admission> {
        let _actor = self.actor.lock().await;
        let _commit = self.placement_fence.lock().await;
        let (name, identity) = (worker.clone(), boot.clone());
        let admission = self
            .db(move |s| {
                let admission = s.admit_worker_identity(&name, &identity, wall_ms())?;
                if !ephemeral && admission == Admission::Conflict {
                    let previous = s.worker_identity(&name)?.unwrap();
                    if previous.rsplit_once('.').map(|(_, instance)| instance)
                        == identity
                            .as_str()
                            .rsplit_once('.')
                            .map(|(_, instance)| instance)
                    {
                        return s.reboot_worker_identity(&name, &identity);
                    }
                    s.forget_worker(&name, wall_ms())?;
                    return s.admit_worker_identity(&name, &identity, wall_ms());
                }
                Ok(admission)
            })
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

    /// Declare an ephemeral worker, or a disconnected member left behind by
    /// rotation, gone. Members wait a lease plus the default TERM grace.
    /// The resident supplies its configured grace through forget_worker_after.
    /// `None` also covers a member already removed from the registration list.
    pub async fn forget_worker(
        &self,
        worker: &WorkerId,
        ephemeral: Option<bool>,
    ) -> Result<std::result::Result<Forgotten, ForgetRefusal>> {
        self.forget_worker_after(
            worker,
            ephemeral,
            crate::supervisor::spec::DEFAULT_TERM_GRACE_MS,
        )
        .await
    }

    pub(crate) async fn forget_worker_after(
        &self,
        worker: &WorkerId,
        ephemeral: Option<bool>,
        term_grace_ms: u64,
    ) -> Result<std::result::Result<Forgotten, ForgetRefusal>> {
        let _actor = self.actor.lock().await;
        let _commit = self.placement_fence.lock().await;
        let connected = self
            .workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(worker);
        if ephemeral != Some(true) {
            use super::member_contact;
            let Some(path) = self.group_key_path() else {
                return Ok(Err(may_forget(ephemeral, connected).unwrap_err()));
            };
            let state = self.store.state_dir();
            let Some(mut contact) =
                member_contact::read(state, worker).map_err(crate::storage::StorageError::from)?
            else {
                return Ok(Err(if ephemeral.is_none() {
                    ForgetRefusal::NotRegistered
                } else {
                    ForgetRefusal::NotEphemeral
                }));
            };
            if connected {
                return Ok(Err(ForgetRefusal::Connected));
            }
            let keys = crate::group::load_keys(path)
                .map_err(|e| crate::storage::StorageError::Invalid(e.to_string()))?;
            if contact.fingerprint == crate::group::fingerprint(&keys.current) {
                return Ok(Err(ForgetRefusal::NotEphemeral));
            }
            let now = wall_ms();
            if contact.open {
                contact.at_ms = now;
                contact.open = false;
                member_contact::write(state, worker, &contact)
                    .map_err(crate::storage::StorageError::from)?;
            }
            let w = worker.clone();
            let lease_ms = self
                .db(move |s| {
                    Ok(s.live_attempts(Some(&w))?
                        .into_iter()
                        .filter_map(|a| a.placement)
                        .filter_map(|p| p.demand["payload"]["lease_ms"].as_u64())
                        .max())
                })
                .await?
                .unwrap_or(self.cfg.lease_ms);
            // 30 seconds covers supervisor polling and forward wall-clock steps
            // of up to 30 seconds while the member is stopping.
            let until = contact
                .at_ms
                .saturating_add(lease_ms)
                .saturating_add(term_grace_ms.max(crate::supervisor::spec::DEFAULT_TERM_GRACE_MS))
                .saturating_add(30_000);
            if now < until {
                return Ok(Err(ForgetRefusal::Waiting {
                    remaining_ms: until - now,
                }));
            }
        } else if let Err(refusal) = may_forget(ephemeral, connected) {
            return Ok(Err(refusal));
        }
        let w = worker.clone();
        Ok(Ok(self
            .db(move |s| {
                let forgotten = s.forget_worker(&w, wall_ms())?;
                s.clear_removed_member(&w)?;
                Ok(forgotten)
            })
            .await?))
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
        attempted: Mutex<Vec<AttemptId>>,
    }
    impl WorkerLink for Clean {
        fn call(&self, request: Request) -> LinkFuture<'_> {
            let attempt = match &request {
                Request::SubmitAttempt(r) => Some(&r.attempt_id),
                Request::GetAttempt(r) => Some(&r.attempt_id),
                Request::RenewLease(r) => Some(&r.attempt_id),
                _ => None,
            };
            if let Some(attempt) = attempt {
                self.attempted.lock().unwrap().push(attempt.clone());
            }
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

    #[tokio::test]
    async fn persistent_reboot_keeps_terminal_collection_open() {
        let f = fx();
        let old = boot("old-boot.01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let new = boot("new-boot.01ARZ3NDEKTSV4RRFFQ69G5FAV");
        f.c.admit_identity(&f.worker, &old).await.unwrap();
        let (_, attempt) = f.placed().await;
        let (_, live) = f.placed().await;
        let done = crate::state::Termination::new(
            Outcome::Succeeded,
            Some(crate::state::Exit::Code(0)),
            None,
        )
        .unwrap();
        f.c.store()
            .record_seen(
                &attempt,
                true,
                Some(crate::storage::Seen {
                    state: AttemptState::Terminal,
                    termination: Some(done),
                    receipts: Default::default(),
                }),
                Observation::Known,
                wall_ms(),
            )
            .unwrap();
        assert_eq!(
            f.c.attach_identity(f.worker.clone(), &new, Arc::new(Clean::default()), false)
                .await
                .unwrap(),
            Admission::Same
        );
        assert_eq!(
            f.c.store().worker_identity(&f.worker).unwrap().as_deref(),
            Some(new.as_str())
        );
        assert!(!f.c.identity_retired(&f.worker, &old).await.unwrap());
        let collectable = f.c.store().collectable(Some(&f.worker), 10).unwrap();
        assert_eq!(collectable.len(), 1);
        assert_eq!(collectable[0].record.attempt_id, attempt);
        assert_eq!(attempt_of(&f, &attempt).termination, Some(done));
        assert!(attempt_of(&f, &live).termination.is_none());
        for id in [&attempt, &live] {
            assert!(!attempt_of(&f, id).retired);
            assert!(f
                .c
                .store()
                .collection(id)
                .unwrap()
                .is_none_or(|c| c.verdict.is_none() && c.closed_at_ms.is_none()));
        }
    }

    #[tokio::test]
    async fn rotated_member_waits_then_settles_and_clears_removal() {
        use super::super::member_contact::{self, Contact};
        let mut f = fx();
        // Placement carries 60s even if this host now uses a shorter lease.
        f.c.cfg.lease_ms = 1000;
        let state = f.c.store().state_dir();
        let path = state.join("cluster.key");
        let old = crate::secrets::key::init(&path).unwrap();
        f.c.set_group_identity(WorkerId::parse("local").unwrap(), path.clone());
        f.c.admit_identity(&f.worker, &boot("old.instance"))
            .await
            .unwrap();
        let (task, attempt) = f.placed().await;
        let (cancelled_task, cancelled) = f.placed().await;
        f.c.cancel_task(&cancelled_task).await.unwrap();
        let mut contact = Contact {
            fingerprint: crate::group::fingerprint(&old),
            at_ms: wall_ms(),
            open: false,
        };
        member_contact::write(state, &f.worker, &contact).unwrap();
        assert_eq!(
            f.c.forget_worker_after(&f.worker, Some(false), 5000)
                .await
                .unwrap(),
            Err(ForgetRefusal::NotEphemeral)
        );
        crate::secrets::key::init(&crate::group::next_key_path(&path)).unwrap();
        crate::group::switch_to_next(&path).unwrap();
        let waiting =
            f.c.forget_worker_after(&f.worker, Some(false), 5000)
                .await
                .unwrap()
                .unwrap_err();
        assert!(
            matches!(waiting, ForgetRefusal::Waiting { remaining_ms } if remaining_ms > 95_000 && remaining_ms <= 100_000)
        );
        assert!(waiting.message().contains("seconds remaining"));
        assert_ne!(attempt_of(&f, &attempt).state, AttemptState::Terminal);
        let link: Arc<dyn WorkerLink> = Arc::new(Clean::default());
        f.c.attach(f.worker.clone(), link.clone()).await;
        assert_eq!(
            f.c.forget_worker_after(&f.worker, Some(false), 5000)
                .await
                .unwrap(),
            Err(ForgetRefusal::Connected)
        );
        f.c.detach_link(&f.worker, &link).await.unwrap();
        contact.at_ms = wall_ms() - 100_001;
        member_contact::write(state, &f.worker, &contact).unwrap();
        f.c.store()
            .record_removed_member(&f.worker, 1, None)
            .unwrap();
        // --remove has already deleted the registration; the durable contact is sufficient.
        let result =
            f.c.forget_worker_after(&f.worker, None, 5000)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(result.identity.as_deref(), Some("old.instance"));
        assert!(result.ended.contains(&attempt));
        assert_eq!(
            attempt_of(&f, &attempt).termination.unwrap().outcome(),
            Outcome::Interrupted
        );
        assert_eq!(
            attempt_of(&f, &cancelled).termination.unwrap().outcome(),
            Outcome::Cancelled
        );
        assert_eq!(
            f.c.store()
                .completion(Target::Task(task))
                .unwrap()
                .collection_pending_tasks,
            0
        );
        assert!(f.c.store().removed_members().unwrap().is_empty());
        assert!(f
            .c
            .identity_retired(&f.worker, &boot("old.instance"))
            .await
            .unwrap());
        let clean = Arc::new(Clean::default());
        assert_eq!(
            f.c.attach_identity(
                f.worker.clone(),
                &boot("old.new-instance"),
                clean.clone(),
                false
            )
            .await
            .unwrap(),
            Admission::First
        );
        f.c.tick().await.unwrap();
        assert_eq!(
            clean.submits.load(Ordering::SeqCst),
            0,
            "old attempts must never reach the new identity"
        );
        assert!(clean.attempted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn crashed_member_session_starts_a_conservative_wait() {
        use super::super::member_contact::{self, Contact};
        let f = fx();
        let state = f.c.store().state_dir();
        let path = state.join("cluster.key");
        crate::secrets::key::init(&path).unwrap();
        f.c.set_group_identity(WorkerId::parse("local").unwrap(), path);
        member_contact::write(
            state,
            &f.worker,
            &Contact {
                fingerprint: "previous-key".into(),
                at_ms: 1,
                open: true,
            },
        )
        .unwrap();
        assert!(matches!(
            f.c.forget_worker_after(&f.worker, Some(false), 5000)
                .await
                .unwrap(),
            Err(ForgetRefusal::Waiting {
                remaining_ms: 100_000
            })
        ));
        assert!(
            !member_contact::read(state, &f.worker)
                .unwrap()
                .unwrap()
                .open
        );
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
