//! One machine as a small cluster: a coordinator and several real worker
//! agents, each with its own state directory, its own supervisors (separate
//! processes) and its own protocol connection.
//!
//! The faults a real cluster has are put in here, in the test crate, and
//! nowhere in the library or the binary:
//!
//! - a request or its answer that is lost, and a link that is gone
//!   ([`Faults`], between the coordinator and one worker);
//! - a worker whose wall clock is off ([`Faults::skew_wall`]: the times it
//!   reports are shifted before the parent sees them);
//! - a kill at a point of the life of an attempt ([`Kill`]): the parent, the
//!   agent, the supervisor, or the whole machine of one worker.
//!
//! What one worker does alone is tested in `worker_agent`, and the
//! coordinator against in-memory workers in `coordinator` and `scheduler`.
//! Here the acceptance items that need several workers run against real ones:
//! A01, A02, A19, A21, A23, A24 (the restart simulated), and A20 and A26 as
//! far as one machine shows them.

use crate::common::{alive, identity_of, kill_recorded_group, read_pid};
use crate::coordinator::{attempts, only, queued, source, spec, Lost};
use crate::worker_agent;
use belt::coordinator::{
    ClientLink, Coordinator, CoordinatorConfig, LinkError, TaskSpec, WorkerLink,
};
use belt::ids::*;
use belt::protocol::transport::duplex_pair;
use belt::protocol::{connect, ClientConfig};
use belt::state::{AttemptState, Outcome, Receipt, Receipts};
use belt::storage::{AttemptRecord, CoordinatorStore, StorageError};
use belt::wire::*;
use belt::worker::serve::serve_connection;
use belt::worker::Agent;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Semaphore;

/// How long a step of a scenario may take before the test gives up. The
/// steps take well under a second each on an idle machine.
const LIMIT: Duration = Duration::from_secs(60);

// ------------------------------------------------------------- the faults

/// What goes wrong between the coordinator and one worker. It outlives the
/// connection: a link made after a restart of the parent or of the agent has
/// the same faults.
#[derive(Default)]
pub(crate) struct Faults {
    lose: Mutex<Vec<(Operation, Lost)>>,
    offline: AtomicBool,
    wall_skew_ms: AtomicI64,
    /// The requests that reached the worker, in order.
    arrived: Mutex<Vec<Operation>>,
}

impl Faults {
    /// Lose the next request of this kind, or the answer to it.
    pub(crate) fn lose(&self, op: Operation, what: Lost) {
        self.lose.lock().unwrap().push((op, what));
    }

    /// Nothing reaches the worker and nothing comes back, until `false`.
    pub(crate) fn offline(&self, off: bool) {
        self.offline.store(off, Ordering::SeqCst);
    }

    /// The worker's wall clock is `ms` ahead of (negative: behind) the
    /// parent's: every time it reports is shifted by that much.
    pub(crate) fn skew_wall(&self, ms: i64) {
        self.wall_skew_ms.store(ms, Ordering::SeqCst);
    }

    /// How many requests of this kind reached the worker.
    pub(crate) fn arrived(&self, op: Operation) -> usize {
        let arrived = self.arrived.lock().unwrap();
        arrived.iter().filter(|o| **o == op).count()
    }

    /// The faults that were asked for and have not happened yet.
    fn pending(&self) -> Vec<(Operation, Lost)> {
        self.lose.lock().unwrap().clone()
    }
}

fn shifted(at_ms: &mut Option<u64>, by: i64) {
    if let Some(at) = at_ms {
        *at = at.saturating_add_signed(by);
    }
}

/// The receipts as a worker whose wall clock is off by `by` stamps them.
/// `collected` is the parent's own and is never in an answer of a worker.
fn skewed_receipts(r: &mut Receipts, by: i64) {
    shifted(&mut r.coordinator_accepted_at_ms, by);
    shifted(&mut r.worker_accepted_at_ms, by);
    shifted(&mut r.command_registered_at_ms, by);
    shifted(&mut r.running_at_ms, by);
    shifted(&mut r.terminal_at_ms, by);
}

fn skewed(mut answer: Response, by: i64) -> Response {
    match &mut answer {
        Response::SubmitAttempt(r) => skewed_receipts(&mut r.receipts, by),
        Response::GetAttempt(r) => {
            skewed_receipts(&mut r.receipts, by);
            shifted(&mut r.spawned_at_ms, by);
        }
        _ => {}
    }
    answer
}

/// The connection to one worker, with its [`Faults`].
struct FaultLink {
    inner: ClientLink,
    faults: Arc<Faults>,
}

pub(crate) async fn connect_agent(
    c: &Coordinator,
    agent: Arc<Agent>,
    faults: Arc<Faults>,
) -> Arc<dyn WorkerLink> {
    let (near, far) = duplex_pair(64 * 1024);
    let cluster = c.store().cluster_id().clone();
    let permits = Arc::new(Semaphore::new(64));
    tokio::spawn(serve_connection(
        agent.clone(),
        far,
        cluster.clone(),
        permits,
    ));
    let membership = belt::protocol::Membership::load(
        agent.config().worker_id.clone(),
        &agent.config().group_key_path,
    )
    .unwrap();
    let cfg = ClientConfig::new(
        cluster,
        c.store().coordinator_id().clone(),
        c.epoch(),
        membership,
    );
    let client = connect(near, cfg).await.unwrap().client;
    Arc::new(FaultLink {
        inner: ClientLink::new(client, Duration::from_secs(30)),
        faults,
    })
}

impl WorkerLink for FaultLink {
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            let op = request.operation();
            if self.faults.offline.load(Ordering::SeqCst) {
                return Err(LinkError::Unconfirmed);
            }
            let lost = {
                let mut lose = self.faults.lose.lock().unwrap();
                let at = lose.iter().position(|(o, _)| *o == op);
                at.map(|i| lose.remove(i).1)
            };
            if lost == Some(Lost::Request) {
                return Err(LinkError::Unconfirmed);
            }
            self.faults.arrived.lock().unwrap().push(op);
            let answer = self.inner.call(request).await;
            if lost == Some(Lost::Answer) {
                return Err(LinkError::Unconfirmed);
            }
            let by = self.faults.wall_skew_ms.load(Ordering::SeqCst);
            answer.map(|a| skewed(a, by))
        })
    }
}

// ------------------------------------------------------------ the workers

/// One worker of the machine: its own state directory and agent.
pub(crate) struct Worker {
    pub(crate) id: WorkerId,
    pub(crate) fx: worker_agent::Fx,
    pub(crate) faults: Arc<Faults>,
    agent: Mutex<Arc<Agent>>,
    boots: Mutex<u32>,
}

impl Worker {
    fn new(name: &str) -> Worker {
        let fx = worker_agent::fx_named(name);
        // What a test asserts is what the parent and the worker do, so no
        // command of a worker waits for the cores of the machine.
        fx.admit_commands(4);
        let agent = fx.start("boot-1");
        Worker {
            id: WorkerId::parse(name).unwrap(),
            fx,
            faults: Arc::default(),
            agent: Mutex::new(agent),
            boots: Mutex::new(1),
        }
    }

    pub(crate) fn agent(&self) -> Arc<Agent> {
        self.agent.lock().unwrap().clone()
    }

    fn boot(&self) -> String {
        format!("boot-{}", self.boots.lock().unwrap())
    }

    /// The attempt as this worker holds it, if it does.
    pub(crate) fn holds(&self, a: &AttemptId) -> Option<AttemptState> {
        self.fx.store().get(a).unwrap().map(|rec| rec.state)
    }

    /// What this worker wrote down about the attempt, for a failure message.
    fn journal(&self, a: &AttemptId) -> Vec<String> {
        let events = self.fx.store().events_of_attempt(a, 200);
        let events = events.unwrap_or_default();
        events
            .iter()
            .map(|e| format!("{}@{} {}", e.kind, e.at_ms, e.payload))
            .collect()
    }
}

/// Where an attempt is cut off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kill {
    /// The agent of the worker ends and is started again; its supervisors
    /// and their commands live on.
    Agent,
    /// The supervisor of the attempt is killed; the agent lives on.
    Supervisor,
    /// The machine of the worker restarts: the agent, the supervisor and
    /// the command are gone, and the next agent has another boot ID.
    Machine,
}

/// A command of a scenario: it records each start and its PID, then does
/// what `then` says. The files are the harness's, whichever worker runs it.
pub(crate) struct Probe {
    pub(crate) task: TaskId,
    starts: PathBuf,
    pidfile: PathBuf,
}

impl Probe {
    pub(crate) fn starts(&self) -> usize {
        std::fs::read_to_string(&self.starts).map_or(0, |s| s.lines().count())
    }

    pub(crate) fn pid(&self) -> Option<i32> {
        read_pid(&self.pidfile)
    }
}

pub(crate) struct OneHost {
    dir: TempDir,
    pub(crate) cfg: CoordinatorConfig,
    pub(crate) workers: Vec<Worker>,
    probes: Mutex<u32>,
    /// The commands a test saw running, each with its start identity.
    seen: Mutex<Vec<(u32, String)>>,
}

impl Drop for OneHost {
    fn drop(&mut self) {
        // A command whose supervisor a test killed is known to no worker
        // when the launch was not on record yet. It is stopped here, by the
        // identity it had when the test saw it, never by its number alone.
        for (pid, start) in self.seen.lock().unwrap().iter() {
            kill_recorded_group(*pid, start);
        }
    }
}

/// A machine with these workers, all started and none attached yet.
pub(crate) fn one_host(names: &[&str]) -> OneHost {
    OneHost {
        dir: crate::test_tmp::tempdir().unwrap(),
        cfg: CoordinatorConfig::default(),
        workers: names.iter().map(|n| Worker::new(n)).collect(),
        probes: Mutex::new(0),
        seen: Mutex::default(),
    }
}

impl OneHost {
    pub(crate) fn worker(&self, name: &str) -> &Worker {
        let found = self.workers.iter().find(|w| w.id.as_str() == name);
        found.unwrap_or_else(|| panic!("no worker {name}"))
    }

    /// One more worker joins the machine.
    pub(crate) fn add(&mut self, name: &str) {
        self.workers.push(Worker::new(name));
    }

    /// Start the parent on its state directory, the first or the next after
    /// the one before was dropped (a kill of the parent), and connect it to
    /// every worker.
    pub(crate) async fn parent(&self) -> Coordinator {
        // The agents of this test fork their supervisors: a child between
        // fork and exec still holds a copy of the lock of the coordinator
        // that was just dropped. Wait that moment out.
        let started = Instant::now();
        let store = loop {
            match CoordinatorStore::open(self.dir.path(), 1) {
                Err(StorageError::Locked(_)) if started.elapsed() < LIMIT => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                other => break other.unwrap(),
            }
        };
        let c = Coordinator::start(store, self.cfg).unwrap();
        for w in &self.workers {
            self.attach(&c, w).await;
        }
        c
    }

    /// A new connection of this parent to the worker's agent of now.
    pub(crate) async fn attach(&self, c: &Coordinator, w: &Worker) {
        let link = connect_agent(c, w.agent(), w.faults.clone()).await;
        c.attach(w.id.clone(), link).await;
    }

    /// A task whose command records its start, then runs `then`.
    pub(crate) fn probe_spec(&self, then: &str) -> (Probe, TaskSpec) {
        let n = {
            let mut probes = self.probes.lock().unwrap();
            *probes += 1;
            *probes
        };
        let probe = Probe {
            task: TaskId::generate(),
            starts: self.dir.path().join(format!("starts-{n}")),
            pidfile: self.dir.path().join(format!("pid-{n}")),
        };
        let script = format!(
            "echo start >> {}; echo $$ > {}; {then}",
            probe.starts.display(),
            probe.pidfile.display()
        );
        let mut task = spec();
        task.commands[0].argv = vec!["/bin/sh".into(), "-c".into(), script];
        let env = &mut task.commands[0].env;
        env.insert("PATH".into(), "/usr/bin:/bin".into());
        (probe, task)
    }

    /// One run of `n` tasks, each doing `then` after it recorded its start.
    pub(crate) async fn enqueue(&self, c: &Coordinator, n: usize, then: &str) -> Vec<Probe> {
        let (probes, specs): (Vec<Probe>, Vec<TaskSpec>) =
            (0..n).map(|_| self.probe_spec(then)).unzip();
        let tasks = probes.iter().map(|p| p.task.clone()).zip(specs).collect();
        c.enqueue(RunId::generate(), source(c), tasks)
            .await
            .unwrap();
        probes
    }

    /// The PID of the probe's command, which runs now. It is remembered
    /// with its identity, and stopped with the harness if it is still there.
    pub(crate) fn running_pid(&self, p: &Probe) -> i32 {
        let pid = p.pid().expect("the command has started");
        let start = identity_of(pid as u32).expect("the running command");
        self.seen.lock().unwrap().push((pid as u32, start));
        pid
    }

    /// The worker has the launch of the attempt on record, with the identity
    /// of the process, and holds the attempt as active. A kill before this
    /// leaves a start nobody can tell, which the worker keeps unresolved
    /// (the `supervisor` tests); the kills of this harness come after it.
    pub(crate) fn launched(&self, w: &Worker, a: &AttemptId) -> bool {
        let launches = w.fx.store().launches(a).unwrap();
        let recorded = launches.iter().any(|(_, _, ident)| ident.is_some());
        recorded && w.holds(a) == Some(AttemptState::Active)
    }

    /// Everything known about the tasks, for a failure message.
    pub(crate) fn picture(&self, c: &Coordinator, probes: &[Probe]) -> String {
        let mut out = String::new();
        for p in probes {
            out.push_str(&format!("task {}: {} start(s)\n", p.task, p.starts()));
            for a in attempts(c, &p.task) {
                out.push_str(&format!(
                    "  parent: {} on {} gen {:?} {:?} retired={} {:?}\n",
                    a.attempt_id, a.worker_id, a.generation, a.state, a.retired, a.termination
                ));
                for w in &self.workers {
                    if let Some(state) = w.holds(&a.attempt_id) {
                        out.push_str(&format!("  {} holds it as {state:?}\n", w.id));
                        for line in w.journal(&a.attempt_id) {
                            out.push_str(&format!("    {line}\n"));
                        }
                    }
                }
            }
        }
        for w in &self.workers {
            let pending = w.faults.pending();
            out.push_str(&format!("{}: faults still pending {pending:?}\n", w.id));
        }
        out
    }

    /// Tick the parent until `done`, and say what is known when it never is.
    pub(crate) async fn tick_until(
        &self,
        c: &Coordinator,
        probes: &[Probe],
        what: &str,
        done: impl Fn() -> bool,
    ) {
        let started = Instant::now();
        loop {
            c.tick().await.unwrap();
            if done() {
                return;
            }
            assert!(
                started.elapsed() < LIMIT,
                "timed out waiting for {what}\n{}",
                self.picture(c, probes)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Tick until every one of the tasks has ended on the parent.
    pub(crate) async fn settle(&self, c: &Coordinator, probes: &[Probe]) {
        let all_ended = || {
            probes.iter().all(|p| {
                let all = attempts(c, &p.task);
                all.last().is_some_and(|a| a.termination.is_some())
            })
        };
        self.tick_until(c, probes, "the tasks to end", all_ended)
            .await;
    }

    /// Cut the attempt off at `kill` on the worker that runs it. The agent
    /// that follows (not after [`Kill::Supervisor`], where the same one
    /// lives on) is connected to `c` again.
    pub(crate) async fn kill(&self, c: &Coordinator, w: &Worker, a: &AttemptId, kill: Kill) {
        assert!(
            self.launched(w, a),
            "{a} is cut off before {} has its launch on record: wait for `launched`",
            w.id
        );
        if kill == Kill::Supervisor {
            w.fx.kill_supervisors(a);
            return;
        }
        // Read while they run: what is killed below is the processes this
        // worker recorded, never whatever has their numbers later.
        let recorded: Vec<(u32, String)> = {
            let launches = w.fx.store().launches(a).unwrap();
            let idents = launches.into_iter().filter_map(|(_, _, i)| i);
            idents.map(|i| (i.pid, i.start_identity)).collect()
        };
        w.agent().shutdown();
        if kill == Kill::Machine {
            w.fx.kill_supervisors(a);
            for (pid, start) in &recorded {
                kill_recorded_group(*pid, start);
            }
            let started = Instant::now();
            while !w.fx.supervisors(a).is_empty() || recorded.iter().any(|(p, _)| alive(*p as i32))
            {
                assert!(
                    started.elapsed() < LIMIT,
                    "the processes of {a} on {} did not die",
                    w.id
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            *w.boots.lock().unwrap() += 1;
        }
        *w.agent.lock().unwrap() = w.fx.start(&w.boot());
        self.attach(c, w).await;
    }
}

/// The worker the one attempt of the task is on.
fn worker_of(c: &Coordinator, p: &Probe) -> String {
    only(c, &p.task).worker_id.to_string()
}

fn outcome(a: &AttemptRecord) -> Option<Outcome> {
    a.termination.as_ref().map(|t| t.outcome())
}

/// Cancel the tasks and see them end: nothing of a scenario is left running.
async fn cancel_all(h: &OneHost, c: &Coordinator, probes: &[Probe]) {
    for p in probes {
        c.cancel_task(&p.task).await.unwrap();
    }
    h.settle(c, probes).await;
}

// -------------------------------------------------------------------- A01

/// A01 against real workers: nobody names a host, and the three tasks run
/// on the parent's own worker and on both children, each command once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a01_three_real_workers_on_one_machine_each_run_one_of_three_tasks() {
    let h = one_host(&["local", "w-a", "w-b"]);
    let c = h.parent().await;
    c.set_parent_local(WorkerId::parse("local").unwrap());
    let probes = h.enqueue(&c, 3, "true").await;
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 3, "{}", h.picture(&c, &probes));
    let mut on: Vec<String> = probes.iter().map(|p| worker_of(&c, p)).collect();
    on.sort();
    assert_eq!(on, ["local", "w-a", "w-b"], "{}", h.picture(&c, &probes));

    h.settle(&c, &probes).await;
    for p in &probes {
        let a = only(&c, &p.task);
        assert_eq!(
            outcome(&a),
            Some(Outcome::Succeeded),
            "{}",
            h.picture(&c, &probes)
        );
        assert_eq!(p.starts(), 1, "{}", h.picture(&c, &probes));
        // The state directories are separate: only the worker the parent
        // chose knows the attempt.
        for w in &h.workers {
            let holds = w.holds(&a.attempt_id).is_some();
            assert_eq!(holds, w.id == a.worker_id, "{}", h.picture(&c, &probes));
        }
    }
}

// -------------------------------------------------------------------- A02

/// A02 against real workers: the backlog the parent kept is taken, and
/// started, by a worker that joins later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a02_a_real_worker_that_joins_later_starts_the_backlog_the_parent_kept() {
    let mut h = one_host(&["w-a"]);
    let c = h.parent().await;
    let probes = h
        .enqueue(&c, 4, &crate::fixture_process::script("exec sleep 300"))
        .await;
    c.tick().await.unwrap();
    c.tick().await.unwrap();
    // One runs and one waits behind it on the only worker (its own limits).
    let kept: Vec<TaskId> = probes[2..].iter().map(|p| p.task.clone()).collect();
    assert_eq!(queued(&c), kept, "{}", h.picture(&c, &probes));
    h.tick_until(&c, &probes, "the first command", || probes[0].starts() == 1)
        .await;

    h.add("w-b");
    h.attach(&c, h.worker("w-b")).await;
    h.tick_until(&c, &probes, "the backlog to be placed", || {
        queued(&c).is_empty()
    })
    .await;
    for p in &probes[2..] {
        assert_eq!(worker_of(&c, p), "w-b", "{}", h.picture(&c, &probes));
    }
    for p in &probes[..2] {
        assert_eq!(worker_of(&c, p), "w-a", "{}", h.picture(&c, &probes));
    }
    // The new worker really starts what it took.
    h.tick_until(&c, &probes, "the new worker to start the backlog", || {
        probes[2..].iter().all(|p| p.starts() == 1)
    })
    .await;
    for p in &probes[2..] {
        let a = only(&c, &p.task).attempt_id;
        assert!(h.worker("w-b").holds(&a).is_some());
        assert_eq!(h.worker("w-a").holds(&a), None);
    }

    cancel_all(&h, &c, &probes).await;
    for p in &probes {
        assert_eq!(attempts(&c, &p.task).len(), 1);
        assert!(p.starts() <= 1, "{}", h.picture(&c, &probes));
    }
}

// --------------------------------------------------------------- A19, A21

/// A19 and A21 with a second worker that is free the whole time: a submit
/// whose answer is lost, and then a link that is gone, never move the task
/// to the other worker, and the command starts once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a19_a21_a_lost_ack_and_a_gone_link_never_start_the_task_on_the_free_worker() {
    let h = one_host(&["w-a", "w-b"]);
    let c = h.parent().await;
    let (a, b) = (h.worker("w-a"), h.worker("w-b"));
    // The first decision goes to `w-a` (equal estimates, the lower id).
    a.faults.lose(Operation::SubmitAttempt, Lost::Answer);
    let probes = h
        .enqueue(&c, 1, &crate::fixture_process::script("exec sleep 300"))
        .await;
    let report = c.tick().await.unwrap();
    let first = only(&c, &probes[0].task);
    assert_eq!(first.worker_id, a.id, "{}", h.picture(&c, &probes));
    assert_eq!(a.faults.arrived(Operation::SubmitAttempt), 1);
    assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
    assert!(a.holds(&first.attempt_id).is_some(), "it did arrive");

    // The parent asks the same worker; the free one is never given the task.
    h.tick_until(&c, &probes, "the acceptance to be known", || {
        let now = only(&c, &probes[0].task);
        now.receipts.reached(Receipt::WorkerAccepted)
    })
    .await;
    h.tick_until(&c, &probes, "the command", || probes[0].pid().is_some())
        .await;
    let pid = h.running_pid(&probes[0]);

    // The link to the worker is gone for many ticks.
    a.faults.offline(true);
    for _ in 0..5 {
        let report = c.tick().await.unwrap();
        assert!(report.placed.is_empty() && report.retired.is_empty());
        assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
    }
    assert!(alive(pid), "the command is not stopped by the silence");
    assert_eq!(attempts(&c, &probes[0].task).len(), 1);
    assert!(queued(&c).is_empty());

    // It returns, and the attempt is followed where it is.
    a.faults.offline(false);
    h.tick_until(&c, &probes, "the attempt to be seen active", || {
        only(&c, &probes[0].task).state == AttemptState::Active
    })
    .await;
    assert_eq!(probes[0].starts(), 1, "{}", h.picture(&c, &probes));
    assert_eq!(b.holds(&first.attempt_id), None);
    assert_eq!(b.faults.arrived(Operation::SubmitAttempt), 0);
    assert_eq!(a.faults.arrived(Operation::SubmitAttempt), 1);

    cancel_all(&h, &c, &probes).await;
    assert_eq!(probes[0].starts(), 1);
    assert!(!alive(pid));
}

// ---------------------------------------------------- A20, A23, A24: kills

/// Two tasks run, one on each worker; the attempt on `w-a` is the one a
/// test cuts off.
struct Running {
    h: OneHost,
    c: Coordinator,
    probes: Vec<Probe>,
    /// The attempt on `w-a` and the PID of its command.
    hit: (AttemptRecord, i32),
    /// The attempt on `w-b` and the PID of its command.
    spared: (AttemptRecord, i32),
}

async fn running_on_both() -> Running {
    let h = one_host(&["w-a", "w-b"]);
    let c = h.parent().await;
    let probes = h
        .enqueue(&c, 2, &crate::fixture_process::script("exec sleep 300"))
        .await;
    h.tick_until(&c, &probes, "both commands on record and active", || {
        probes.iter().all(|p| {
            let a = only(&c, &p.task);
            let w = h.worker(a.worker_id.as_str());
            p.pid().is_some() && a.state == AttemptState::Active && h.launched(w, &a.attempt_id)
        })
    })
    .await;
    let of = |name: &str| {
        let p = probes.iter().find(|p| worker_of(&c, p) == name);
        let p = p.unwrap_or_else(|| panic!("nothing on {name}\n{}", h.picture(&c, &probes)));
        (only(&c, &p.task), h.running_pid(p))
    };
    let (hit, spared) = (of("w-a"), of("w-b"));
    Running {
        h,
        c,
        probes,
        hit,
        spared,
    }
}

impl Running {
    fn probe_of(&self, a: &AttemptRecord) -> &Probe {
        self.probes.iter().find(|p| p.task == a.task_id).unwrap()
    }

    /// The other worker and its command were never touched.
    fn the_other_is_untouched(&self) {
        let picture = self.h.picture(&self.c, &self.probes);
        assert!(alive(self.spared.1), "{picture}");
        assert_eq!(self.probe_of(&self.spared.0).starts(), 1, "{picture}");
        assert_eq!(attempts(&self.c, &self.spared.0.task_id).len(), 1);
    }

    async fn end(self) {
        cancel_all(&self.h, &self.c, &self.probes).await;
        for p in &self.probes {
            assert_eq!(p.starts(), 1, "{}", self.h.picture(&self.c, &self.probes));
        }
        assert!(!alive(self.hit.1) && !alive(self.spared.1));
    }
}

/// A23: the parent is killed and the next one, in a new epoch, adopts what
/// runs on both workers and starts nothing again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a23_a_killed_parent_is_followed_by_one_that_adopts_what_two_real_workers_run() {
    let mut r = running_on_both().await;
    drop(r.c);
    r.c = r.h.parent().await;
    assert_eq!(r.c.epoch(), CoordinatorEpoch(2));
    for _ in 0..3 {
        let report = r.c.tick().await.unwrap();
        assert!(report.placed.is_empty() && report.retired.is_empty());
    }
    let picture = r.h.picture(&r.c, &r.probes);
    for (a, pid) in [&r.hit, &r.spared] {
        assert!(alive(*pid), "{picture}");
        assert_eq!(attempts(&r.c, &a.task_id).len(), 1, "{picture}");
        assert_eq!(only(&r.c, &a.task_id).state, AttemptState::Active);
        assert_eq!(r.probe_of(a).starts(), 1, "{picture}");
    }
    r.end().await;
}

/// A23: the agent of one worker is killed and started again. It adopts the
/// supervisor that lived on; the parent follows the same attempt over the
/// new connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a23_a_restarted_agent_of_one_worker_keeps_its_command_and_the_parent_follows_it() {
    let r = running_on_both().await;
    let w = r.h.worker("w-a");
    r.h.kill(&r.c, w, &r.hit.0.attempt_id, Kill::Agent).await;
    assert!(!w.agent().startup_report().boot_changed);
    for _ in 0..3 {
        let report = r.c.tick().await.unwrap();
        assert!(
            report.placed.is_empty() && report.retired.is_empty() && report.unknown.is_empty(),
            "{report:?}\n{}",
            r.h.picture(&r.c, &r.probes)
        );
    }
    assert!(alive(r.hit.1), "{}", r.h.picture(&r.c, &r.probes));
    assert_eq!(only(&r.c, &r.hit.0.task_id).state, AttemptState::Active);
    assert_eq!(r.probe_of(&r.hit.0).starts(), 1);
    r.the_other_is_untouched();
    r.end().await;
}

/// A20 as far as one machine shows it: the supervisor of a running command
/// is killed. The worker ends the attempt once, as not succeeded, only after
/// the command is gone; the parent settles it once and does not start the
/// task again by itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a20_a_killed_supervisor_ends_the_attempt_once_and_nothing_is_started_again() {
    let r = running_on_both().await;
    let w = r.h.worker("w-a");
    let a = r.hit.0.attempt_id.clone();
    r.h.kill(&r.c, w, &a, Kill::Supervisor).await;
    let hit = std::slice::from_ref(r.probe_of(&r.hit.0));
    r.h.settle(&r.c, hit).await;
    let picture = r.h.picture(&r.c, &r.probes);
    let ended = only(&r.c, &r.hit.0.task_id);
    assert_eq!(outcome(&ended), Some(Outcome::Interrupted), "{picture}");
    // The budget is not freed while a descendant is left: the command is
    // gone by the time the attempt has ended.
    assert!(!alive(r.hit.1), "{picture}");
    let store = w.fx.store();
    let ends = store.events_of(&a, "attempt_terminal").unwrap();
    assert_eq!(ends.len(), 1, "{picture}");
    // The group of the command was stopped before the end was written.
    let recovered = store.events_of(&a, "supervisor_recover_node").unwrap();
    assert_eq!(recovered.len(), 1, "{picture}");
    assert_eq!(recovered[0].payload["group_stopped"], true, "{picture}");
    assert!(recovered[0].seq < ends[0].seq, "{picture}");
    for _ in 0..3 {
        let report = r.c.tick().await.unwrap();
        assert!(report.placed.is_empty(), "{report:?}\n{picture}");
    }
    assert_eq!(attempts(&r.c, &r.hit.0.task_id).len(), 1, "{picture}");
    assert_eq!(r.probe_of(&r.hit.0).starts(), 1, "{picture}");
    r.the_other_is_untouched();
    r.end().await;
}

/// A24, the restart simulated: the machine of one worker restarts (its
/// processes are killed and the next agent has another boot ID). Its attempt
/// ends `interrupted` there and on the parent; the other worker goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a24_a_restart_of_one_workers_machine_interrupts_its_attempt_and_spares_the_other() {
    let r = running_on_both().await;
    let w = r.h.worker("w-a");
    let a = r.hit.0.attempt_id.clone();
    r.h.kill(&r.c, w, &a, Kill::Machine).await;
    assert!(w.agent().startup_report().boot_changed);
    let hit = std::slice::from_ref(r.probe_of(&r.hit.0));
    r.h.settle(&r.c, hit).await;
    let picture = r.h.picture(&r.c, &r.probes);
    let ended = only(&r.c, &r.hit.0.task_id);
    assert_eq!(outcome(&ended), Some(Outcome::Interrupted), "{picture}");
    assert!(!alive(r.hit.1), "{picture}");
    assert_eq!(r.probe_of(&r.hit.0).starts(), 1, "{picture}");
    r.the_other_is_untouched();
    r.end().await;
}

// -------------------------------------------------------------------- A26

/// A26 as far as one machine shows it: the wall clocks of the workers are
/// off, one a day behind and one a year ahead. The parent neither decides by
/// nor keeps the times a worker reports: the leases are renewed past one
/// period, both commands run to their end, and each is settled once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a26_workers_whose_wall_clocks_are_off_keep_their_leases_and_end_normally() {
    const DAY: i64 = 24 * 3600 * 1000;
    let mut h = one_host(&["w-a", "w-b"]);
    // The commands outlive one lease period (6 s > 4 s) and live on only by
    // the renewals of the ticks, one every 50 ms: an eighty-fold margin.
    h.cfg.lease_ms = 4_000;
    h.cfg.worker_budget_ms = 1_000;
    let c = h.parent().await;
    h.worker("w-a").faults.skew_wall(-DAY);
    h.worker("w-b").faults.skew_wall(365 * DAY);
    let before = belt::platform::wall_ms();
    let probes = h
        .enqueue(&c, 2, &crate::fixture_process::script("sleep 6"))
        .await;
    h.settle(&c, &probes).await;
    let picture = h.picture(&c, &probes);
    let mut on = Vec::new();
    for p in &probes {
        let a = only(&c, &p.task);
        assert_eq!(outcome(&a), Some(Outcome::Succeeded), "{picture}");
        assert_eq!(p.starts(), 1, "{picture}");
        // Every time the parent keeps is from its own clock: the times a
        // worker tells are a day and a year away, and none of them is here.
        let kept = [
            Receipt::WorkerAccepted,
            Receipt::CommandRegistered,
            Receipt::Running,
            Receipt::Terminal,
        ];
        for receipt in kept {
            let at = a.receipts.at_ms(receipt);
            let at = at.unwrap_or_else(|| {
                panic!(
                    "{receipt:?} was not reached
{picture}"
                )
            });
            let near = at >= before && at < before + DAY as u64 / 2;
            assert!(
                near,
                "{receipt:?} at {at}, started at {before}
{picture}"
            );
        }
        on.push(a.worker_id.to_string());
    }
    on.sort();
    assert_eq!(on, ["w-a", "w-b"], "{picture}");
    for w in &h.workers {
        assert!(w.faults.arrived(Operation::RenewLease) > 0, "{picture}");
    }
}
