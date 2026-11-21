//! Symmetric members on one machine (spec chapter 24): two or three members
//! of one group, each with its own parent (a coordinator on its own state
//! directory) and its own real worker agent. Every parent reaches its own
//! worker and the other members' under its own member name, with the key
//! file its own agent proves with, as a resident does. A member's resident
//! is that pair: stopping it drops the parent and shuts its agent down. A
//! run is enqueued on the parent, as `belt ci` and `submit` do through the
//! resident.
//!
//! A51, A52 as far as the one-worker tests (`coordinator::shared_admission`,
//! `worker_agent::group_membership`) do not show it, A53 and A54.

use super::*;
use crate::common::identity_of;
use crate::coordinator::{queued, source, spec};
use belt::error::{ErrorCode, GroupRefusal};
use belt::protocol::{ConnectError, Connected, Membership};
use belt::scheduler::EVENT_TASK_WAITING;
use belt::secrets::key::ClusterKey;
use belt::storage::WorkerStore;
use std::path::Path;

/// A key no member holds.
fn other_key() -> ClusterKey {
    ClusterKey::from_bytes(vec![9; 32]).unwrap()
}

/// One member: its worker agent, and its parent while its resident runs.
struct Member {
    id: WorkerId,
    fx: worker_agent::Fx,
    agent: Arc<Agent>,
    /// The admission window of its worker, of every member together.
    window: usize,
    dir: TempDir,
    parent: Option<Coordinator>,
}

impl Member {
    /// The key file its agent proves with, and its parent dials with.
    fn key(&self) -> PathBuf {
        self.agent.config().group_key_path.clone()
    }

    fn c(&self) -> &Coordinator {
        let running = self.parent.as_ref();
        running.unwrap_or_else(|| panic!("the resident of {} is stopped", self.id))
    }

    fn store(&self) -> WorkerStore {
        self.fx.store()
    }

    /// The worker holds the attempt as active, with its launch on record.
    fn runs(&self, a: &AttemptId) -> bool {
        let launches = self.store().launches(a).unwrap();
        let recorded = launches.iter().any(|(_, _, ident)| ident.is_some());
        let held = self.store().get(a).unwrap().map(|rec| rec.state);
        recorded && held == Some(AttemptState::Active)
    }

    /// The command of the attempt as this worker recorded it.
    fn command(&self, a: &AttemptId) -> (u32, String) {
        let launches = self.store().launches(a).unwrap();
        let ident = launches.into_iter().find_map(|(_, _, i)| i).unwrap();
        (ident.pid, ident.start_identity)
    }

    /// How the worker ended the attempt, if it did.
    fn ended(&self, a: &AttemptId) -> Option<Outcome> {
        let rec = self.store().get(a).unwrap()?;
        rec.termination.map(|t| t.outcome())
    }

    fn noted(&self, a: &AttemptId, kind: &str) -> usize {
        self.store().events_of(a, kind).unwrap().len()
    }
}

/// A command of a scenario, which records each of its starts.
struct Probe {
    member: usize,
    task: TaskId,
    starts: PathBuf,
}

impl Probe {
    fn starts(&self) -> usize {
        std::fs::read_to_string(&self.starts).map_or(0, |s| s.lines().count())
    }
}

struct Group {
    /// The group every worker serves.
    cluster: ClusterId,
    cfg: CoordinatorConfig,
    members: Vec<Member>,
    dir: TempDir,
    tasks: Mutex<Vec<(usize, TaskId)>>,
}

/// Members of one group with these names, their agents started and no
/// resident yet. `window` is the admission window of every worker (the
/// default holds 1 + 1).
fn group(names: &[&str], window: Option<u32>) -> Group {
    let members = names.iter().map(|name| {
        let fx = worker_agent::fx_named(name);
        fx.admit_commands(4);
        let agent = match window {
            Some(w) => fx.start_holding("boot-1", w),
            None => fx.start("boot-1"),
        };
        Member {
            id: WorkerId::parse(name).unwrap(),
            fx,
            agent,
            window: window.map_or(2, |w| w as usize),
            dir: crate::test_tmp::tempdir().unwrap(),
            parent: None,
        }
    });
    Group {
        cluster: ClusterId::generate(),
        cfg: CoordinatorConfig::default(),
        members: members.collect(),
        dir: crate::test_tmp::tempdir().unwrap(),
        tasks: Mutex::default(),
    }
}

impl Group {
    fn c(&self, i: usize) -> &Coordinator {
        self.members[i].c()
    }

    /// Start the resident of member `i`, which reaches the workers `to`.
    async fn start(&mut self, i: usize, to: &[usize]) {
        let store = CoordinatorStore::open(self.members[i].dir.path(), 1).unwrap();
        let c = Coordinator::start(store, self.cfg).unwrap();
        c.set_parent_local(self.members[i].id.clone());
        self.members[i].parent = Some(c);
        for &j in to {
            self.link(i, j).await;
        }
    }

    /// Every resident, each reaching every worker, its own included.
    async fn start_all(&mut self) {
        let all: Vec<usize> = (0..self.members.len()).collect();
        for i in 0..self.members.len() {
            self.start(i, &all).await;
        }
    }

    /// Stop the resident of member `i`: its parent and its agent.
    fn stop(&mut self, i: usize) {
        drop(self.members[i].parent.take());
        self.members[i].agent.shutdown();
    }

    /// A connection to the worker of member `to`, by whoever claims this
    /// group, coordinator, name and key.
    async fn dial(
        &self,
        cluster: &ClusterId,
        c: &Coordinator,
        name: &WorkerId,
        key: &Path,
        to: usize,
    ) -> Result<Connected, ConnectError> {
        let (near, far) = duplex_pair(64 * 1024);
        tokio::spawn(serve_connection(
            self.members[to].agent.clone(),
            far,
            self.cluster.clone(),
            Arc::new(Semaphore::new(64)),
        ));
        let cfg = ClientConfig::new(
            cluster.clone(),
            c.store().coordinator_id().clone(),
            c.epoch(),
            Membership::load(name.clone(), key).unwrap(),
        );
        connect(near, cfg).await
    }

    /// Member `from`'s connection to member `to`'s worker.
    async fn connected(&self, from: usize, to: usize) -> Result<Connected, ConnectError> {
        let m = &self.members[from];
        self.dial(&self.cluster, m.c(), &m.id, &m.key(), to).await
    }

    /// Member `from`'s parent reaches member `to`'s worker (again).
    async fn link(&self, from: usize, to: usize) {
        let connected = self.connected(from, to).await;
        let connected = connected.unwrap_or_else(|e| panic!("{from} to {to}: {e}"));
        let link = ClientLink::new(connected.client, Duration::from_secs(30));
        let to = self.members[to].id.clone();
        self.c(from).attach(to, Arc::new(link)).await;
    }

    /// One run of `n` tasks on member `i`, each doing `then` after it
    /// recorded its start.
    async fn submit(&self, i: usize, n: usize, then: &str) -> Vec<Probe> {
        let mut tasks = self.tasks.lock().unwrap().len();
        let mut probes = Vec::new();
        let mut specs = Vec::new();
        for _ in 0..n {
            tasks += 1;
            let starts = self.dir.path().join(format!("starts-{tasks}"));
            let mut task = spec();
            let script = format!("echo start >> {}; {then}", starts.display());
            task.commands[0].argv = vec!["/bin/sh".into(), "-c".into(), script];
            let env = &mut task.commands[0].env;
            env.insert("PATH".into(), "/usr/bin:/bin".into());
            let p = Probe {
                member: i,
                task: TaskId::generate(),
                starts,
            };
            self.tasks.lock().unwrap().push((i, p.task.clone()));
            specs.push((p.task.clone(), task));
            probes.push(p);
        }
        let c = self.c(i);
        c.enqueue(RunId::generate(), source(c), specs)
            .await
            .unwrap();
        probes
    }

    /// The one attempt of the probe's task, on the parent that submitted it.
    fn only(&self, p: &Probe) -> AttemptRecord {
        crate::coordinator::only(self.c(p.member), &p.task)
    }

    /// How the probe's task ended on the parent that submitted it.
    fn ended(&self, p: &Probe) -> Option<Outcome> {
        let all = attempts(self.c(p.member), &p.task);
        all.last().and_then(outcome)
    }

    fn worker(&self, name: &WorkerId) -> &Member {
        let m = self.members.iter().find(|m| &m.id == name);
        m.unwrap_or_else(|| panic!("no member {name}"))
    }

    /// No worker holds more attempts that are not terminal than its own
    /// window, whoever submitted them.
    fn check_windows(&self) {
        for m in &self.members {
            let held = m.store().list_nonterminal().unwrap();
            assert!(
                held.len() <= m.window,
                "{} holds {} attempts\n{}",
                m.id,
                held.len(),
                self.picture()
            );
        }
    }

    /// Everything known about the tasks, for a failure message.
    fn picture(&self) -> String {
        let mut out = String::new();
        for (i, task) in self.tasks.lock().unwrap().iter() {
            let Some(c) = self.members[*i].parent.as_ref() else {
                out.push_str(&format!("task {task} of the stopped {}\n", *i));
                continue;
            };
            out.push_str(&format!("task {task} of {}\n", self.members[*i].id));
            for a in attempts(c, task) {
                out.push_str(&format!(
                    "  {} on {} {:?} retired={} {:?}\n",
                    a.attempt_id, a.worker_id, a.state, a.retired, a.termination
                ));
            }
        }
        for m in &self.members {
            for rec in m.store().list_attempts(None, true, 100).unwrap() {
                out.push_str(&format!(
                    "{} holds {} of {} as {:?}\n",
                    m.id, rec.attempt_id, rec.coordinator_id, rec.state
                ));
            }
        }
        out
    }

    /// Tick every running parent until `done`; each worker's window holds
    /// after every round.
    async fn tick_until(&self, what: &str, done: impl Fn(&Group) -> bool) {
        let started = Instant::now();
        loop {
            for m in &self.members {
                if let Some(c) = &m.parent {
                    c.tick().await.unwrap();
                }
            }
            self.check_windows();
            if done(self) {
                return;
            }
            assert!(
                started.elapsed() < LIMIT,
                "timed out waiting for {what}\n{}",
                self.picture()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn settle(&self, probes: &[&Probe]) {
        let all_ended = |g: &Group| probes.iter().all(|p| g.ended(p).is_some());
        self.tick_until("the tasks to end", all_ended).await;
    }

    /// The probe's task succeeded, its command started once, and only the
    /// member that submitted it has a record of it.
    fn succeeded_once(&self, p: &Probe) {
        assert_eq!(
            self.ended(p),
            Some(Outcome::Succeeded),
            "{}",
            self.picture()
        );
        assert_eq!(p.starts(), 1, "{}", self.picture());
        // Placed more than once only when a worker refused it for capacity
        // (members placing at once): what was refused never started.
        let mut all = attempts(self.c(p.member), &p.task);
        let a = all.pop().unwrap();
        for refused in &all {
            assert!(
                refused.retired && refused.termination.is_none(),
                "{refused:?}"
            );
        }
        let submitter = self.c(p.member).store().coordinator_id().clone();
        let held = self
            .worker(&a.worker_id)
            .store()
            .get(&a.attempt_id)
            .unwrap();
        assert_eq!(held.unwrap().coordinator_id, submitter);
        for (j, m) in self.members.iter().enumerate() {
            let Some(c) = m.parent.as_ref().filter(|_| j != p.member) else {
                continue;
            };
            assert!(attempts(c, &p.task).is_empty(), "{} has {}", m.id, p.task);
            assert!(!queued(c).contains(&p.task));
        }
    }
}

// -------------------------------------------------------------------- A51

/// A51: three members, each reaching all three workers. A run from any of
/// them runs on its own worker and on the other two, and only that member
/// has its record. Runs the three put in at once all complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a51_a_run_from_any_member_runs_on_its_own_worker_and_on_the_others() {
    let mut g = group(&["m-a", "m-b", "m-c"], None);
    g.start_all().await;
    for i in 0..3 {
        let probes = g.submit(i, 3, "true").await;
        let report = g.c(i).tick().await.unwrap();
        assert_eq!(report.placed.len(), 3, "{}", g.picture());
        let mut on: Vec<String> = probes
            .iter()
            .map(|p| g.only(p).worker_id.to_string())
            .collect();
        on.sort();
        assert_eq!(on, ["m-a", "m-b", "m-c"], "{}", g.picture());
        let all: Vec<&Probe> = probes.iter().collect();
        g.settle(&all).await;
        for p in &probes {
            g.succeeded_once(p);
        }
    }

    // The three members put in a run each at the same moment.
    let mut runs = Vec::new();
    for i in 0..3 {
        runs.extend(g.submit(i, 2, "true").await);
    }
    let (a, b, c) = tokio::join!(g.c(0).tick(), g.c(1).tick(), g.c(2).tick());
    for report in [a, b, c] {
        report.unwrap();
    }
    let all: Vec<&Probe> = runs.iter().collect();
    g.settle(&all).await;
    for p in &runs {
        g.succeeded_once(p);
    }
}

// -------------------------------------------------------------------- A52

/// A52, what the one-worker tests leave out: a member's own worker is full
/// with another member's work. Its run neither fails nor goes past the
/// window: one task goes to the other member's worker, the other waits for
/// capacity and is placed on its own worker once that has room.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a52_a_members_own_worker_full_of_anothers_work_sends_its_run_elsewhere_or_waits() {
    let mut g = group(&["m-a", "m-b"], Some(1));
    // m-b reaches only m-a's worker, and fills it.
    g.start(1, &[0]).await;
    let stay = crate::fixture_process::script("exec sleep 300");
    let theirs = g.submit(1, 1, &stay).await;
    g.tick_until("m-b's command on m-a's worker", |_| theirs[0].starts() == 1)
        .await;
    assert_eq!(g.only(&theirs[0]).worker_id.as_str(), "m-a");

    // m-a reaches both workers and puts in two tasks.
    g.start(0, &[0, 1]).await;
    let mine = g.submit(0, 2, &stay).await;
    let started = |_: &Group| mine.iter().filter(|p| p.starts() == 1).count();
    g.tick_until("one of m-a's commands", |g| started(g) == 1)
        .await;
    for _ in 0..5 {
        g.tick_until("a round", |_| true).await;
    }
    assert_eq!(started(&g), 1, "{}", g.picture());
    let (running, waiting) = if mine[0].starts() == 1 {
        (&mine[0], &mine[1])
    } else {
        (&mine[1], &mine[0])
    };
    let placed = attempts(g.c(0), &running.task).pop().unwrap();
    assert_eq!(placed.worker_id.as_str(), "m-b", "{}", g.picture());
    // m-a's own worker holds m-b's attempt and nothing of m-a's.
    let held = g.members[0].store().list_nonterminal().unwrap();
    assert_eq!(held.len(), 1, "{}", g.picture());
    assert_eq!(held[0].coordinator_id, *g.c(1).store().coordinator_id());
    // The other task did not fail: it waits, for capacity. What it was
    // placed with and refused is closed without a start.
    assert_eq!(
        queued(g.c(0)),
        vec![waiting.task.clone()],
        "{}",
        g.picture()
    );
    for a in attempts(g.c(0), &waiting.task) {
        assert!(a.retired && a.termination.is_none(), "{a:?}");
    }
    let waits = g.c(0).store().events_after(0, 10_000).unwrap();
    let waits: Vec<_> = waits
        .iter()
        .filter(|e| e.kind == EVENT_TASK_WAITING)
        .collect();
    let last = waits.last().expect("why the task waits is recorded");
    assert_eq!(last.payload["task"], waiting.task.to_string());
    assert_eq!(last.payload["blocked_reason"], "capacity");

    // m-b's work ends: m-a's own worker has room, and the task that waited
    // starts there, once.
    g.c(1).cancel_task(&theirs[0].task).await.unwrap();
    g.tick_until("the task that waited", |_| waiting.starts() == 1)
        .await;
    let later = attempts(g.c(0), &waiting.task).pop().unwrap();
    assert!(!later.retired);
    assert_eq!(later.worker_id.as_str(), "m-a", "{}", g.picture());
    assert_eq!(g.ended(&theirs[0]), Some(Outcome::Cancelled));
    for p in &mine {
        g.c(0).cancel_task(&p.task).await.unwrap();
    }
    g.settle(&[&mine[0], &mine[1]]).await;
    for p in mine.iter().chain(&theirs) {
        assert_eq!(p.starts(), 1, "{}", g.picture());
    }
}

// -------------------------------------------------------------------- A53

/// A53: a parent of another group, and one of this group with another key,
/// are refused by every member's worker. The refusal names no worker and no
/// identity, the worker records nothing of them, and their run is never
/// placed. The members' runs go on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a53_another_group_or_a_wrong_key_is_refused_by_every_member_and_gets_no_attempt() {
    let mut g = group(&["m-a", "m-b"], None);
    g.start_all().await;
    let runs = [
        g.submit(0, 1, &crate::fixture_process::script("sleep 1"))
            .await,
        g.submit(1, 1, &crate::fixture_process::script("sleep 1"))
            .await,
    ];
    let keys = crate::test_tmp::tempdir().unwrap();
    let (same, wrong) = (keys.path().join("same.key"), keys.path().join("wrong.key"));
    crate::proto_common::place_group_key(&same);
    crate::proto_common::place_key(&wrong, &other_key());
    let outsiders = [
        (ClusterId::generate(), &same, "other group"),
        (g.cluster.clone(), &wrong, "wrong key"),
    ];
    for (cluster, key, what) in outsiders {
        let dir = crate::test_tmp::tempdir().unwrap();
        let o = Coordinator::start(CoordinatorStore::open(dir.path(), 1).unwrap(), g.cfg).unwrap();
        let starts = dir.path().join("starts");
        let mut task = spec();
        let script = format!("echo start >> {}", starts.display());
        task.commands[0].argv = vec!["/bin/sh".into(), "-c".into(), script];
        let id = TaskId::generate();
        let tasks = vec![(id.clone(), task)];
        o.enqueue(RunId::generate(), source(&o), tasks)
            .await
            .unwrap();
        let name = WorkerId::parse("m-x").unwrap();
        for to in 0..2 {
            let refused = match g.dial(&cluster, &o, &name, key, to).await {
                Err(ConnectError::Refused(e)) => e,
                Err(e) => panic!("{what}: refused otherwise: {e}"),
                Ok(_) => panic!("{what}: let in by {}", g.members[to].id),
            };
            assert_eq!(refused.code, ErrorCode::ProtocolIncompatible, "{what}");
            let why = refused.group_refusal.clone().expect("a reason");
            match what {
                "other group" => assert_eq!(why, GroupRefusal::GroupMismatch),
                _ => assert!(matches!(why, GroupRefusal::GroupKeyMismatch { .. })),
            }
            // Nothing of the worker before a proof: not its name, not its
            // identity.
            let said = serde_json::to_string(&refused).unwrap();
            for secret in ["m-a", "m-b", "boot-1"] {
                assert!(!said.contains(secret), "{what}: {secret} in {said}");
            }
        }
        o.tick().await.unwrap();
        assert_eq!(queued(&o), vec![id.clone()], "{what}");
        assert!(attempts(&o, &id).is_empty(), "{what}");
        let outsider = o.store().coordinator_id().to_string();
        for m in &g.members {
            let members = m.store().members().unwrap();
            assert!(members.iter().all(|(_, c)| *c != outsider), "{what}");
            let held = m.store().list_attempts(None, true, 100).unwrap();
            assert!(held
                .iter()
                .all(|a| a.coordinator_id.to_string() != outsider));
        }
        assert!(!starts.exists(), "{what}");
    }
    g.settle(&[&runs[0][0], &runs[1][0]]).await;
    for p in runs.iter().flatten() {
        g.succeeded_once(p);
    }
}

/// A53: a member left out of a switch of the key is refused by the members
/// that remain. Its attempt on their worker gets no renewal and is stopped
/// when its lease runs out; the remaining member's run on the same worker
/// ends normally. Its own parent keeps the attempt unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a53_a_member_removed_by_a_key_switch_is_refused_and_its_attempt_ends_with_its_lease() {
    let mut g = group(&["m-a", "m-b", "m-c"], None);
    g.cfg.lease_ms = 4_000;
    g.cfg.worker_budget_ms = 1_000;
    // m-a and m-c place on m-a's worker; m-b is the other that remains.
    g.start(0, &[0]).await;
    g.start(1, &[0, 1]).await;
    g.start(2, &[0]).await;
    let removed = g
        .submit(2, 1, &crate::fixture_process::script("exec sleep 300"))
        .await;
    let kept = g
        .submit(0, 1, &crate::fixture_process::script("sleep 12"))
        .await;
    let on_a = |g: &Group, p: &Probe| {
        let a = attempts(g.c(p.member), &p.task).pop();
        a.is_some_and(|a| a.worker_id.as_str() == "m-a" && g.members[0].runs(&a.attempt_id))
    };
    g.tick_until("both commands on m-a's worker", |g| {
        on_a(g, &removed[0]) && on_a(g, &kept[0])
    })
    .await;
    let theirs = g.only(&removed[0]).attempt_id;
    let command = g.members[0].command(&theirs);

    // m-a and m-b switch to a key m-c never gets: first the next key on
    // both, then the switch.
    for m in &g.members[..2] {
        crate::proto_common::place_key(&belt::group::next_key_path(&m.key()), &other_key());
        m.agent.tick();
    }
    for m in &g.members[..2] {
        assert!(belt::group::switch_to_next(&m.key()).unwrap());
        m.agent.tick();
    }
    // The members that remain reach each other again with the new key.
    g.link(0, 0).await;
    g.link(1, 0).await;
    g.link(1, 1).await;
    for to in 0..2 {
        match g.connected(2, to).await {
            Err(ConnectError::Refused(e)) => assert_eq!(
                e.group_refusal,
                Some(GroupRefusal::GroupKeyMismatch {
                    fingerprint: belt::group::fingerprint(&other_key())
                })
            ),
            Err(e) => panic!("refused otherwise: {e}"),
            Ok(_) => panic!("the removed member was let in by {}", g.members[to].id),
        }
    }

    g.tick_until(
        "the removed member's attempt to stop, the other to end",
        |g| g.members[0].ended(&theirs).is_some() && g.ended(&kept[0]).is_some(),
    )
    .await;
    assert_eq!(g.members[0].ended(&theirs), Some(Outcome::Interrupted));
    assert_eq!(g.members[0].noted(&theirs, "lease_expired"), 1);
    assert_ne!(identity_of(command.0).as_deref(), Some(command.1.as_str()));
    g.succeeded_once(&kept[0]);
    let mine = g.only(&kept[0]).attempt_id;
    assert_eq!(g.members[0].noted(&mine, "lease_expired"), 0);
    // The removed member does not learn the end: its attempt stays unknown.
    assert!(g.only(&removed[0]).termination.is_none(), "{}", g.picture());
    assert_eq!(removed[0].starts(), 1);
}

/// A53: a member that told a worker it retired that worker's identity is
/// refused by it from then on, and only its attempts there stop, without
/// waiting for their lease. The other member's attempt there goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a53_a_member_that_retired_a_workers_identity_is_refused_and_only_its_attempts_stop() {
    let mut g = group(&["m-a", "m-b"], None);
    g.start(0, &[1]).await;
    g.start(1, &[1]).await;
    let stay = crate::fixture_process::script("exec sleep 300");
    let retired = g.submit(0, 1, &stay).await;
    let other = g.submit(1, 1, &stay).await;
    let on_b = |g: &Group, p: &Probe| {
        let a = attempts(g.c(p.member), &p.task).pop();
        a.is_some_and(|a| g.members[1].runs(&a.attempt_id))
    };
    g.tick_until("both commands on m-b's worker", |g| {
        on_b(g, &retired[0]) && on_b(g, &other[0])
    })
    .await;
    let (mine, theirs) = (g.only(&retired[0]).attempt_id, g.only(&other[0]).attempt_id);
    let command = g.members[1].command(&theirs);

    // m-a retires the identity m-b's worker answers with: it keeps no link
    // to it and tells the worker once.
    let b = g.members[1].id.clone();
    g.c(0).detach(&b).await.unwrap();
    let connected = g.connected(0, 1).await.unwrap();
    let told = Instant::now();
    let boot = connected.worker.worker_boot_id.clone();
    belt::coordinator::identity::notify_retired(g.c(0), &connected.client, &b, &boot).await;
    g.tick_until("m-a's attempt to stop", |g| {
        g.members[1].ended(&mine).is_some()
    })
    .await;
    assert_eq!(g.members[1].ended(&mine), Some(Outcome::Interrupted));
    // Well before the lease of 60 s it was given would have run out.
    let lease = Duration::from_millis(g.cfg.lease_ms);
    assert!(told.elapsed() < lease / 2, "{:?}", told.elapsed());
    match g.connected(0, 1).await {
        Err(ConnectError::Refused(e)) => {
            assert_eq!(e.group_refusal, Some(GroupRefusal::IdentityRetired))
        }
        Err(e) => panic!("refused otherwise: {e}"),
        Ok(_) => panic!("the member that retired the identity was let in"),
    }
    // m-b's attempt on the same worker is untouched, and m-b still gets in.
    assert!(g.members[1].runs(&theirs), "{}", g.picture());
    assert_eq!(identity_of(command.0).as_deref(), Some(command.1.as_str()));
    g.link(1, 1).await;
    g.c(1).cancel_task(&other[0].task).await.unwrap();
    g.settle(&[&other[0]]).await;
    assert_eq!(g.ended(&other[0]), Some(Outcome::Cancelled));
    assert_eq!((retired[0].starts(), other[0].starts()), (1, 1));
}

// -------------------------------------------------------------------- A54

/// A54: two members place on one worker, and the resident of one of them
/// stops. Its attempt runs on within its lease and is then stopped by the
/// worker; the other member's run on the same worker ends normally, and a
/// run it puts in after the stop runs there too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a54_a_stopped_member_stalls_only_its_own_runs_and_the_others_end_on_the_same_worker() {
    let mut g = group(&["m-a", "m-b"], None);
    g.cfg.lease_ms = 4_000;
    g.cfg.worker_budget_ms = 1_000;
    g.start(0, &[1]).await;
    g.start(1, &[1]).await;
    let stalled = g
        .submit(0, 1, &crate::fixture_process::script("exec sleep 300"))
        .await;
    let going = g
        .submit(1, 1, &crate::fixture_process::script("sleep 8"))
        .await;
    let on_b = |g: &Group, p: &Probe| {
        let a = attempts(g.c(p.member), &p.task).pop();
        a.is_some_and(|a| a.worker_id.as_str() == "m-b" && g.members[1].runs(&a.attempt_id))
    };
    g.tick_until("both commands on m-b's worker", |g| {
        on_b(g, &stalled[0]) && on_b(g, &going[0])
    })
    .await;
    let (hit, spared) = (g.only(&stalled[0]).attempt_id, g.only(&going[0]).attempt_id);
    let command = g.members[1].command(&hit);

    g.stop(0);
    // Right after the stop, the stopped member's attempt still runs.
    assert!(g.members[1].runs(&hit), "{}", g.picture());
    assert_eq!(identity_of(command.0).as_deref(), Some(command.1.as_str()));

    g.tick_until("its attempt to stop and the other run to end", |g| {
        g.members[1].ended(&hit).is_some() && g.ended(&going[0]).is_some()
    })
    .await;
    let w = &g.members[1];
    assert_eq!(w.ended(&hit), Some(Outcome::Interrupted), "{}", g.picture());
    assert_eq!(w.noted(&hit, "lease_expired"), 1);
    assert_ne!(identity_of(command.0).as_deref(), Some(command.1.as_str()));
    g.succeeded_once(&going[0]);
    assert_eq!(w.noted(&spared, "lease_expired"), 0);
    assert_eq!(stalled[0].starts(), 1);

    let after = g.submit(1, 1, "true").await;
    g.settle(&[&after[0]]).await;
    g.succeeded_once(&after[0]);
    assert_eq!(g.only(&after[0]).worker_id.as_str(), "m-b");
}
