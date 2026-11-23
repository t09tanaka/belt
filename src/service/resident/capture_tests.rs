//! Spec 25.4 (b): what the resident answers to the enqueue and the cancel of
//! a run that has no row, and what it deletes while answering.
use super::tests::{delivery_resident, delivery_spec};
use super::*;
use crate::platform::owned_temp::OwnerRecord;
use crate::service::completion::Target;
use crate::source::retention::{AbandonCursor, AbandonScope};
use crate::source::{CaptureParams, SourceRequest};
use std::collections::BTreeMap;

struct Captured {
    run: RunId,
    source: SourceId,
    digest: Digest,
}

fn input(root: &Path) -> PathBuf {
    let input = root.join("original");
    if !input.exists() {
        std::fs::create_dir(&input).unwrap();
        std::fs::write(input.join("file"), b"original").unwrap();
    }
    input
}

/// The capture of a submitting command, owned by `owner` (`None`: by no row,
/// as a binary from before the owner row made it).
fn capture(resident: &Resident, root: &Path, owner: Option<&OwnerRecord>) -> Captured {
    let run = RunId::generate();
    let sources = &resident.captured.sources;
    if let Some(owner) = owner {
        sources.own_capture_as(&run, owner).unwrap();
    }
    let got = crate::secrets::capture::capture_run(
        &BTreeMap::new(),
        sources,
        &resident.captured.vault,
        &crate::secrets::capture::SecretInputs {
            key: &resident.captured.current_key().unwrap(),
            declared: &[],
        },
        &run,
        &SourceRequest::Directory { dir: input(root) },
        &CaptureParams::default(),
    )
    .unwrap();
    let digest = Digest::parse(&got.manifest.manifest_digest)
        .or_else(|_| Digest::parse(&format!("sha256:{}", got.manifest.manifest_digest)))
        .unwrap();
    crate::workspace::in_place::bind(
        &resident.captured.parent_state,
        &run,
        &got.manifest.source_id,
        &digest,
        &resident.worker_id,
        &input(root),
        vec![],
    )
    .unwrap();
    Captured {
        run,
        source: got.manifest.source_id,
        digest,
    }
}

fn me() -> OwnerRecord {
    OwnerRecord::this_process().unwrap()
}

/// A process of this test that stands for the submitting command: alive
/// until it is killed (only this test's own child is ever signalled).
struct Command {
    child: std::process::Child,
}
impl Command {
    fn start() -> Self {
        let child = std::process::Command::new("/bin/sleep")
            .arg("300")
            .spawn()
            .unwrap();
        Self { child }
    }
    fn record(&self) -> OwnerRecord {
        let crate::platform::StartIdentity::Identity(start) =
            crate::platform::process_start_identity(self.child.id())
        else {
            panic!("the start of this test's own child is readable");
        };
        OwnerRecord {
            pid: self.child.id(),
            start,
            ..me()
        }
    }
    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}
impl Drop for Command {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

async fn ask(resident: &Resident, operation: Operation) -> Reply {
    let (mut client, job) = send(resident, operation).await;
    let reply: Reply = read_message_with_timeout(&mut client, Duration::from_secs(20))
        .await
        .unwrap();
    job.await.unwrap().unwrap();
    reply
}

/// Send a request and leave its answer unread: the stream and the handler.
async fn send(
    resident: &Resident,
    operation: Operation,
) -> (UnixStream, tokio::task::JoinHandle<anyhow::Result<()>>) {
    let (mut client, server) = UnixStream::pair().unwrap();
    let (stop, _keep) = watch::channel(false);
    let job = tokio::spawn(handle(
        server,
        resident.coordinator.clone(),
        resident.sources.clone(),
        resident.captured.clone(),
        resident.worker_client.clone(),
        resident.worker_id.clone(),
        resident.client.clone(),
        Arc::new(Mutex::new(false)),
        stop,
        mpsc::channel(1).0,
    ));
    write_message(
        &mut client,
        &super::Request {
            schema_version: 6,
            cluster_id: resident.client.cluster_id.clone(),
            owner_token: resident.client.owner_token.clone(),
            operation,
        },
    )
    .await
    .unwrap();
    (client, job)
}

fn submit(captured: &Captured) -> Operation {
    Operation::SubmitCaptured {
        run_id: captured.run.clone(),
        source_id: captured.source.clone(),
        tasks: vec![(
            TaskId::generate(),
            delivery_spec(captured.digest.clone(), false),
        )],
    }
}
fn cancel(run: &RunId) -> Operation {
    Operation::Cancel {
        target: Target::Run(run.clone()),
        expected_control_revision: 0,
        request_id: RunId::generate().to_string(),
    }
}

/// Every row of the parent database that names the run, and the rows of the
/// source index, as counts: (run, cancel intent, receipts, events, owner).
fn rows(resident: &Resident, run: &RunId) -> (i64, i64, i64, i64, i64) {
    let id = run.to_string();
    let (a, b, c, d) = resident
        .coordinator
        .store()
        .db
        .call(move |c| {
            let count = |table: &str| -> crate::storage::Result<i64> {
                Ok(c.query_row(
                    &format!("SELECT count(*) FROM {table} WHERE run_id=?1"),
                    [&id],
                    |r| r.get(0),
                )?)
            };
            Ok((
                count("run")?,
                count("run_cancel_intent")?,
                count("control_request")?,
                count("event")?,
            ))
        })
        .unwrap();
    let owner = i64::from(
        resident
            .captured
            .sources
            .capture_owner(run)
            .unwrap()
            .is_some(),
    );
    (a, b, c, d, owner)
}

/// The files the run ID names, and the object and manifest files of the store.
fn files(resident: &Resident, run: &RunId) -> Vec<PathBuf> {
    let state = &resident.captured.parent_state;
    let mut found: Vec<PathBuf> = [
        state.join("sources/runs").join(run.as_str()),
        state.join("sources/runs").join(format!("{run}.lock")),
        state.join("sources/secrets").join(format!("{run}.json")),
        state.join("secrets").join(format!("{run}.json")),
        state.join("in-place").join(format!("{run}.json")),
    ]
    .into_iter()
    .filter(|path| std::fs::symlink_metadata(path).is_ok())
    .collect();
    for dir in ["sources/manifests", "sources/objects"] {
        let mut stack = vec![state.join(dir)];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).into_iter().flatten() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    found.push(path);
                }
            }
        }
    }
    found
}

/// No object of the store is deleted before the discovery of manifests from
/// before the index is over; the driver's first passes do this in a resident.
fn registration_done(resident: &Resident) {
    let mut cursor = crate::source::retention::PreviewCursor::default();
    for _ in 0..8 {
        if resident
            .captured
            .sources
            .retention_registration_page(resident.coordinator.store(), &mut cursor)
            .unwrap()
            .registration_done
        {
            return;
        }
    }
    panic!("registration did not finish");
}

fn pass(resident: &Resident) -> crate::source::retention::AbandonReport {
    resident
        .captured
        .sources
        .abandon_captures(
            resident.coordinator.store(),
            &resident.captured.vault,
            AbandonScope::All,
            &mut AbandonCursor::default(),
        )
        .unwrap()
}

/// What `doctor` counts: (owners elsewhere, run records, in-place bindings,
/// entries that are not a run's).
fn unjudged(resident: &Resident) -> (usize, usize, usize, usize) {
    let store = resident.coordinator.store();
    let counts =
        crate::service::doctor::unjudged_captures(&resident.captured.parent_state, &|run| {
            store.has_run(run).unwrap()
        })
        .unwrap()
        .unwrap();
    (
        counts.elsewhere,
        counts.records,
        counts.bindings,
        counts.unnamed,
    )
}

fn not_found(reply: &Reply) -> bool {
    matches!(reply, Reply::ControlError { error } if error.code == crate::error::ErrorCode::NotFound)
}

/// (ii) and the hand-over: while the owner lives a pass deletes nothing, the
/// enqueue that follows is accepted, and the accepted run takes the capture
/// over: the owner row goes, everything else stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_enqueue_of_a_capture_in_progress_is_accepted_and_takes_the_capture_over() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    registration_done(&resident);
    let captured = capture(&resident, root.path(), Some(&me()));
    let before = files(&resident, &captured.run);
    assert_eq!(pass(&resident).in_progress, 1);
    assert_eq!(files(&resident, &captured.run), before);
    let reply = ask(&resident, submit(&captured)).await;
    assert!(matches!(reply, Reply::Accepted { .. }), "{reply:?}");
    assert!(matches!(rows(&resident, &captured.run), (1, 0, 0, _, 0)));
    assert_eq!(files(&resident, &captured.run), before);
    // A later pass has nothing to do with the run.
    assert_eq!(pass(&resident).examined, 0);
    assert_eq!(files(&resident, &captured.run), before);
}

/// The enqueue of a run whose capture no owner row claims is refused with
/// `NOT_FOUND` (`capture_abandoned`) and creates nothing. (vii) What such a
/// capture left is not deleted: nothing proves it is not in progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_enqueue_without_an_owner_row_is_refused_and_its_capture_is_kept() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    registration_done(&resident);
    let captured = capture(&resident, root.path(), None);
    let before = files(&resident, &captured.run);
    let reply = ask(&resident, submit(&captured)).await;
    assert!(not_found(&reply), "{reply:?}");
    assert!(
        matches!(&reply, Reply::ControlError { error } if error.message.contains(CAPTURE_ABANDONED))
    );
    assert_eq!(rows(&resident, &captured.run), (0, 0, 0, 0, 0));
    assert!(resident
        .coordinator
        .store()
        .queued_tasks(1)
        .unwrap()
        .is_empty());
    assert_eq!(files(&resident, &captured.run), before);
    assert_eq!(pass(&resident).examined, 0);
    assert_eq!(files(&resident, &captured.run), before);
    // `doctor` counts what nothing can decide about.
    assert_eq!(unjudged(&resident), (0, 1, 1, 0));
    // A capture that a run or an owner row claims is not counted.
    let owned = capture(&resident, root.path(), Some(&me()));
    assert_eq!(unjudged(&resident), (0, 1, 1, 0));
    assert!(matches!(
        ask(&resident, submit(&owned)).await,
        Reply::Accepted { .. }
    ));
    assert_eq!(unjudged(&resident), (0, 1, 1, 0));
}

/// (iv) A refused enqueue deletes what its capture left in the request that
/// refused it: no pass of the driver runs in this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_enqueue_deletes_its_capture_in_the_same_request() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    registration_done(&resident);
    let captured = capture(&resident, root.path(), Some(&me()));
    assert!(!files(&resident, &captured.run).is_empty());
    let refused = Captured {
        digest: Digest::of_bytes(b"another manifest"),
        ..captured
    };
    let reply = ask(&resident, submit(&refused)).await;
    assert!(
        matches!(&reply, Reply::Error { message } if message.contains("digest")),
        "{reply:?}"
    );
    assert_eq!(rows(&resident, &refused.run), (0, 0, 0, 0, 0));
    assert_eq!(files(&resident, &refused.run), Vec::<PathBuf>::new());
    // And the same run is not found afterwards, whatever is sent for it.
    let again = ask(&resident, submit(&refused)).await;
    assert!(not_found(&again), "{again:?}");
}

/// A capture whose owner gave up is refused and deleted in that request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_enqueue_of_a_capture_its_owner_gave_up_is_refused_and_deleted() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    registration_done(&resident);
    let captured = capture(&resident, root.path(), Some(&me()));
    resident
        .captured
        .sources
        .give_up_capture(&captured.run)
        .unwrap();
    let reply = ask(&resident, submit(&captured)).await;
    assert!(not_found(&reply), "{reply:?}");
    assert_eq!(rows(&resident, &captured.run), (0, 0, 0, 0, 0));
    assert_eq!(files(&resident, &captured.run), Vec::<PathBuf>::new());
}

/// (viii) The cancel of a run ID that no owner row names is `NOT_FOUND` and
/// adds no row to the parent database.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_of_a_run_nobody_is_capturing_is_not_found_and_writes_nothing() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    let total = |resident: &Resident| -> i64 {
        resident
            .coordinator
            .store()
            .db
            .call(|c| {
                Ok(c.query_row(
                    "SELECT (SELECT count(*) FROM run_cancel_intent)+(SELECT count(*) FROM control_request)+(SELECT count(*) FROM event)+(SELECT count(*) FROM run)",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap()
    };
    let before = total(&resident);
    let run = RunId::generate();
    let reply = ask(&resident, cancel(&run)).await;
    assert!(not_found(&reply), "{reply:?}");
    // A capture owned somewhere this resident cannot see is not accepted
    // either, and is not deleted.
    let other = OwnerRecord {
        place: format!("another-host-{}", me().place),
        ..me()
    };
    let elsewhere = capture(&resident, root.path(), Some(&other));
    let kept = files(&resident, &elsewhere.run);
    let reply = ask(&resident, cancel(&elsewhere.run)).await;
    assert!(not_found(&reply), "{reply:?}");
    assert_eq!(total(&resident), before);
    assert_eq!(files(&resident, &elsewhere.run), kept);
    assert_eq!(rows(&resident, &elsewhere.run), (0, 0, 0, 0, 1));
    // (v) Neither does a pass delete it, and `doctor` counts the row.
    assert_eq!(pass(&resident).elsewhere, 1);
    assert_eq!(files(&resident, &elsewhere.run), kept);
    assert_eq!(unjudged(&resident), (1, 0, 0, 0));
}

/// (ix) The cancel of a capture in progress is accepted and recorded; when
/// the command is then killed, the next passes delete the capture and the
/// three rows of the cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_during_a_capture_is_accepted_and_its_rows_go_with_the_killed_command() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    registration_done(&resident);
    let mut command = Command::start();
    let captured = capture(&resident, root.path(), Some(&command.record()));
    let reply = ask(&resident, cancel(&captured.run)).await;
    assert!(
        matches!(&reply, Reply::Cancelled { snapshot } if !snapshot.accepted && snapshot.cancel_requested),
        "{reply:?}"
    );
    assert_eq!(rows(&resident, &captured.run), (0, 1, 1, 1, 1));
    // Still in progress: a pass leaves the capture and the rows.
    assert_eq!(pass(&resident).in_progress, 1);
    assert_eq!(rows(&resident, &captured.run), (0, 1, 1, 1, 1));
    command.kill();
    for _ in 0..4 {
        if pass(&resident).continuing == 0 {
            break;
        }
    }
    assert_eq!(rows(&resident, &captured.run), (0, 0, 0, 0, 0));
    assert_eq!(files(&resident, &captured.run), Vec::<PathBuf>::new());
}

/// (x) An enqueue that arrives after the cancel of its capture becomes a run
/// that is already cancelled, as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_enqueue_after_the_cancel_of_its_capture_is_a_cancelled_run() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    let captured = capture(&resident, root.path(), Some(&me()));
    let reply = ask(&resident, cancel(&captured.run)).await;
    assert!(matches!(reply, Reply::Cancelled { .. }), "{reply:?}");
    let reply = ask(&resident, submit(&captured)).await;
    assert!(matches!(reply, Reply::Accepted { .. }), "{reply:?}");
    let id = captured.run.to_string();
    let (tasks, cancelled): (i64, i64) = resident
        .coordinator
        .store()
        .db
        .call(move |c| {
            Ok(c.query_row(
                "SELECT count(*),count(cancel_requested_at_ms) FROM task WHERE run_id=?1",
                [&id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?)
        })
        .unwrap();
    assert_eq!((tasks, cancelled), (1, 1));
    assert!(matches!(rows(&resident, &captured.run), (1, 1, 1, _, 0)));
}

/// (xii) A cancel for a capture its owner gave up is `NOT_FOUND`, writes
/// nothing, and the capture is deleted in that request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_after_the_owner_gave_up_is_not_found_and_deletes_the_capture() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    registration_done(&resident);
    let captured = capture(&resident, root.path(), Some(&me()));
    resident
        .captured
        .sources
        .give_up_capture(&captured.run)
        .unwrap();
    let reply = ask(&resident, cancel(&captured.run)).await;
    assert!(not_found(&reply), "{reply:?}");
    assert_eq!(rows(&resident, &captured.run), (0, 0, 0, 0, 0));
    assert_eq!(files(&resident, &captured.run), Vec::<PathBuf>::new());
}

/// (iii) and (xi), the order "the abandonment first": the command sent its
/// request and ended; the abandonment holds the source lock when the request
/// arrives. The request waits for the lock, finds no owner row, is refused
/// and writes nothing: everything is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_that_loses_to_the_abandonment_is_refused_and_everything_is_gone() {
    for cancelling in [false, true] {
        let root = crate::test_tmp::tempdir().unwrap();
        let resident = delivery_resident(root.path()).await;
        registration_done(&resident);
        let mut command = Command::start();
        let captured = capture(&resident, root.path(), Some(&command.record()));
        command.kill();
        let (entered, resume) = resident.captured.sources.pause_retention_page();
        let (coordinator, inputs) = (resident.coordinator.clone(), resident.captured.clone());
        let abandonment = std::thread::spawn(move || {
            inputs
                .sources
                .abandon_captures(
                    coordinator.store(),
                    &inputs.vault,
                    AbandonScope::All,
                    &mut AbandonCursor::default(),
                )
                .unwrap()
        });
        entered.recv().unwrap();
        let operation = if cancelling {
            cancel(&captured.run)
        } else {
            submit(&captured)
        };
        let (reply, report) = tokio::join!(ask(&resident, operation), async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            resume.send(()).unwrap();
            tokio::task::spawn_blocking(move || abandonment.join().unwrap())
                .await
                .unwrap()
        });
        assert_eq!(report.finished, 1, "{report:?}");
        assert!(not_found(&reply), "cancelling={cancelling}: {reply:?}");
        assert_eq!(rows(&resident, &captured.run), (0, 0, 0, 0, 0));
        assert_eq!(files(&resident, &captured.run), Vec::<PathBuf>::new());
    }
}

/// (iii), the order "the enqueue first": the command sent its enqueue and
/// ended. The run is accepted, and the abandonment that follows finds the
/// run and deletes nothing of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_enqueue_that_wins_against_the_abandonment_is_a_whole_run() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    registration_done(&resident);
    let mut command = Command::start();
    let captured = capture(&resident, root.path(), Some(&command.record()));
    let before = files(&resident, &captured.run);
    command.kill();
    let reply = ask(&resident, submit(&captured)).await;
    assert!(matches!(reply, Reply::Accepted { .. }), "{reply:?}");
    let report = pass(&resident);
    assert_eq!((report.finished, report.paths), (0, 0), "{report:?}");
    assert!(matches!(rows(&resident, &captured.run), (1, 0, 0, _, 0)));
    assert_eq!(files(&resident, &captured.run), before);
}

/// A run whose row exists but whose owner row was not deleted (the resident
/// died between the commit and the hand-over) loses only the owner row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_owner_row_left_after_the_commit_goes_and_the_run_stays() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    registration_done(&resident);
    let mut command = Command::start();
    let captured = capture(&resident, root.path(), Some(&command.record()));
    let before = files(&resident, &captured.run);
    let reply = ask(&resident, submit(&captured)).await;
    assert!(matches!(reply, Reply::Accepted { .. }), "{reply:?}");
    resident
        .captured
        .sources
        .own_capture_as(&captured.run, &command.record())
        .unwrap();
    command.kill();
    assert_eq!(pass(&resident).handed_off, 1);
    assert!(matches!(rows(&resident, &captured.run), (1, 0, 0, _, 0)));
    assert_eq!(files(&resident, &captured.run), before);
}

/// The cancel of a run without a row lets go of the admission lock before it
/// reads the owner row, and an enqueue can commit the run and release the
/// row in between. Then "no row", "abandoned" and "the row cannot be read"
/// say nothing about the run: the cancel looks for the run again and cancels
/// it. Without that the command would be told `NOT_FOUND` for the run it
/// just submitted, and the run would go on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_that_overlaps_the_commit_of_its_run_cancels_the_run() {
    for case in ["released", "abandoned", "busy"] {
        let root = crate::test_tmp::tempdir().unwrap();
        let resident = delivery_resident(root.path()).await;
        registration_done(&resident);
        let mut command = Command::start();
        let captured = capture(&resident, root.path(), Some(&command.record()));
        let before = files(&resident, &captured.run);
        // "abandoned": the owner is gone when the row is read, and the
        // enqueue it sent commits after that read. The others: the enqueue
        // commits, and releases the row, before the row is read.
        let point = if case == "abandoned" {
            command.kill();
            "cancel_before_answer"
        } else {
            "cancel_before_owner"
        };
        let (mut paused, resume) = resident.captured.retention_test.pause_at(point);
        let (mut client, job) = send(&resident, cancel(&captured.run)).await;
        paused.recv().await.unwrap();
        let reply = ask(&resident, submit(&captured)).await;
        assert!(matches!(reply, Reply::Accepted { .. }), "{case}: {reply:?}");
        assert!(matches!(rows(&resident, &captured.run), (1, 0, 0, _, 0)));
        // "busy": a deletion holds the source lock for longer than the
        // cancel waits for it.
        let other = (case == "busy").then(|| {
            let mut gone = Command::start();
            let dead = capture(&resident, root.path(), Some(&gone.record()));
            gone.kill();
            let (entered, release) = resident.captured.sources.pause_retention_page();
            let (coordinator, inputs) = (resident.coordinator.clone(), resident.captured.clone());
            let abandonment = std::thread::spawn(move || {
                inputs
                    .sources
                    .abandon_captures(
                        coordinator.store(),
                        &inputs.vault,
                        AbandonScope::Run(&dead.run),
                        &mut AbandonCursor::default(),
                    )
                    .unwrap()
            });
            entered.recv().unwrap();
            (release, abandonment)
        });
        resume.send(()).unwrap();
        let reply: Reply = read_message_with_timeout(&mut client, Duration::from_secs(20))
            .await
            .unwrap();
        if let Some((release, abandonment)) = other {
            release.send(()).unwrap();
            tokio::task::spawn_blocking(move || abandonment.join().unwrap())
                .await
                .unwrap();
        }
        job.await.unwrap().unwrap();
        assert!(
            matches!(&reply, Reply::Cancelled { snapshot } if snapshot.accepted && snapshot.cancel_requested),
            "{case}: {reply:?}"
        );
        assert!(matches!(rows(&resident, &captured.run), (1, 1, 1, _, 0)));
        // The run took the capture over: the cancel deleted nothing of it.
        assert_eq!(files(&resident, &captured.run), before, "{case}");
    }
}

/// The answer to a refusal is written before the deletion of the capture
/// begins: the deletion waits for locks and syncs every unlink, and the
/// client waits five seconds for its answer. Here the deletion stops at its
/// start, holding the source lock, until the answer was read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_is_answered_before_the_capture_is_deleted() {
    for cancelling in [true, false] {
        let root = crate::test_tmp::tempdir().unwrap();
        let resident = delivery_resident(root.path()).await;
        registration_done(&resident);
        let captured = capture(&resident, root.path(), Some(&me()));
        resident
            .captured
            .sources
            .give_up_capture(&captured.run)
            .unwrap();
        let before = files(&resident, &captured.run);
        let (entered, release) = resident.captured.sources.pause_retention_page();
        let operation = if cancelling {
            cancel(&captured.run)
        } else {
            submit(&captured)
        };
        let (mut client, job) = send(&resident, operation).await;
        let reply: Reply = read_message_with_timeout(&mut client, Duration::from_secs(5))
            .await
            .expect("the answer does not wait for the deletion");
        assert!(not_found(&reply), "cancelling={cancelling}: {reply:?}");
        // The deletion runs in the same request, after the answer.
        tokio::task::spawn_blocking(move || entered.recv().unwrap())
            .await
            .unwrap();
        assert_eq!(files(&resident, &captured.run), before);
        assert_eq!(rows(&resident, &captured.run), (0, 0, 0, 0, 1));
        release.send(()).unwrap();
        job.await.unwrap().unwrap();
        assert_eq!(rows(&resident, &captured.run), (0, 0, 0, 0, 0));
        assert_eq!(files(&resident, &captured.run), Vec::<PathBuf>::new());
    }
}

/// An enqueue that cannot take the source lock in time is refused as busy,
/// and the answer does not ask for the same request again: the refusal ends
/// the capture, so only the command can be run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_enqueue_refused_as_busy_is_told_to_run_the_command_again() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    registration_done(&resident);
    let captured = capture(&resident, root.path(), Some(&me()));
    let mut gone = Command::start();
    let dead = capture(&resident, root.path(), Some(&gone.record()));
    gone.kill();
    let (entered, release) = resident.captured.sources.pause_retention_page();
    let (coordinator, inputs) = (resident.coordinator.clone(), resident.captured.clone());
    let abandonment = std::thread::spawn(move || {
        inputs
            .sources
            .abandon_captures(
                coordinator.store(),
                &inputs.vault,
                AbandonScope::Run(&dead.run),
                &mut AbandonCursor::default(),
            )
            .unwrap()
    });
    entered.recv().unwrap();
    let (mut client, job) = send(&resident, submit(&captured)).await;
    let reply: Reply = read_message_with_timeout(&mut client, Duration::from_secs(20))
        .await
        .unwrap();
    release.send(()).unwrap();
    tokio::task::spawn_blocking(move || abandonment.join().unwrap())
        .await
        .unwrap();
    job.await.unwrap().unwrap();
    let Reply::ControlError { error } = &reply else {
        panic!("{reply:?}");
    };
    assert_eq!(error.code, crate::error::ErrorCode::SourceBusy);
    assert!(
        error.message.contains("run the command again") && !error.message.contains("retry"),
        "{}",
        error.message
    );
    assert_eq!(rows(&resident, &captured.run).0, 0);
}
