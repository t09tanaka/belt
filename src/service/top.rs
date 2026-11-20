//! Read-only live view of coordinator task state and resident connections.
use crate::error::{ApiError, ErrorCode};
use crate::ids::WorkerId;
use crate::storage::{query::MonitorTask, CoordinatorStore};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::Paragraph;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, IsTerminal};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub(crate) fn interval(value: &str) -> Result<Duration, String> {
    value
        .parse::<f64>()
        .ok()
        .filter(|v| (0.2..=3600.0).contains(v))
        .map(Duration::from_secs_f64)
        .ok_or_else(|| "expected seconds between 0.2 and 3600".into())
}

#[derive(Serialize)]
struct Snapshot {
    observed_at_ms: u64,
    workers: BTreeMap<WorkerId, &'static str>,
    tasks: Vec<MonitorTask>,
    resident_observed: bool,
}

fn read() -> Result<Snapshot, ApiError> {
    let env = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    let spec = super::context::runtime_spec(&env)?;
    let store = CoordinatorStore::open_read_only(&spec.state_dir).map_err(|e| {
        ApiError::new(
            ErrorCode::NotFound,
            format!("cannot read coordinator state: {e}"),
        )
    })?;
    if spec
        .expected_cluster
        .as_ref()
        .is_some_and(|cluster| cluster != store.cluster_id())
    {
        return Err(ApiError::new(
            ErrorCode::Conflict,
            "coordinator cluster identity changed",
        ));
    }
    let tasks = store
        .monitor_tasks()
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    let config =
        crate::config::load::load(&spec.env, spec.repo_root.as_deref(), &Default::default())
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
    let mut names = BTreeSet::new();
    if config.local_worker.value {
        names.insert(spec.worker_id.clone());
    }
    for id in config
        .workers
        .iter()
        .map(|w| w.id.as_str())
        .chain(tasks.iter().filter_map(|t| t.worker_id.as_deref()))
    {
        names.insert(
            WorkerId::parse(id)
                .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?,
        );
    }
    let observed = super::workers::observed_connections(&spec, names.iter().cloned().collect())?;
    let workers = names
        .into_iter()
        .map(|id| {
            let state = match observed.as_ref().and_then(|m| m.get(&id)) {
                Some((true, _)) => "connected",
                Some((false, _)) => "disconnected",
                None => "unknown",
            };
            (id, state)
        })
        .collect();
    Ok(Snapshot {
        observed_at_ms: crate::platform::wall_ms(),
        workers,
        tasks,
        resident_observed: observed.is_some(),
    })
}

// Both data labels and error messages can contain terminal controls.
fn plain(text: &str) -> String {
    text.chars().map(|c| if c.is_control() || matches!(c, '\u{2028}' | '\u{2029}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}') { '�' } else { c }).collect()
}

fn section(task: &MonitorTask) -> &'static str {
    if task.observation.as_deref().is_some_and(|o| o != "known") {
        "Uncertain"
    } else if matches!(
        task.state.as_str(),
        "queued" | "held" | "accepted" | "preparing" | "ready"
    ) {
        "Waiting"
    } else {
        "Active"
    }
}

fn lines(s: &Snapshot) -> Vec<Line<'static>> {
    let heading = |title: String| Line::styled(title, Style::default().fg(Color::Cyan).bold());
    let mut out = vec![heading(format!("Workers ({})", s.workers.len()))];
    if !s.resident_observed {
        out.push(Line::from(
            "Resident unavailable; task states are last recorded",
        ));
    }
    for (name, state) in &s.workers {
        out.push(Line::from(format!("  {}  {state}", plain(name.as_str()))));
    }
    for title in ["Active", "Waiting", "Uncertain"] {
        let rows: Vec<_> = s.tasks.iter().filter(|t| section(t) == title).collect();
        out.push(heading(format!("{title} ({})", rows.len())));
        for row in rows {
            let status = if title == "Uncertain" {
                row.observation.as_deref().unwrap_or("unknown")
            } else {
                &row.state
            };
            out.push(Line::from(format!(
                "  {}  {}  {}",
                plain(&row.task_id),
                plain(status),
                plain(row.worker_id.as_deref().unwrap_or("unplaced"))
            )));
            out.push(Line::from(format!(
                "    {}  {}  {}",
                plain(row.command.as_deref().unwrap_or("")),
                plain(&row.run_id),
                plain(row.label.as_deref().unwrap_or(""))
            )));
        }
    }
    out
}

struct Restore;
impl Drop for Restore {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

fn interactive(refresh: Duration) -> io::Result<()> {
    // The reader can wait for the resident; it never owns the terminal.
    // Dropping the request channel stops it after any in-flight sample.
    let (ask, requests) = mpsc::channel();
    let (send, updates) = mpsc::channel();
    std::thread::Builder::new()
        .name("top-reader".into())
        .spawn(move || {
            while requests.recv().is_ok() {
                if send.send(read()).is_err() {
                    break;
                }
            }
        })?;
    let _restore = Restore;
    let mut terminal = ratatui::try_init()?;
    let mut next = Instant::now();
    let mut pending = false;
    let mut view = vec![Line::from("Reading coordinator state…")];
    let mut error = None;
    let mut offset: usize = 0;
    let mut redraw = true;
    loop {
        if !pending && Instant::now() >= next {
            ask.send(()).map_err(io::Error::other)?;
            pending = true;
        }
        match updates.try_recv() {
            Ok(result) => {
                match result {
                    Ok(snapshot) => {
                        view = lines(&snapshot);
                        error = None;
                    }
                    Err(e) => error = Some(plain(&e.message)),
                }
                pending = false;
                next = Instant::now() + refresh;
                redraw = true;
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                return Err(io::Error::other("coordinator reader stopped"))
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if redraw {
            terminal.draw(|f| {
                let [head, body, foot] = Layout::vertical([
                    Constraint::Length(1),
                    Constraint::Min(0),
                    Constraint::Length(1),
                ])
                .areas(f.area());
                f.render_widget(
                    Paragraph::new(format!("belt top   every {}s", refresh.as_secs_f64()))
                        .style(Style::default().bold()),
                    head,
                );
                offset = offset.min(view.len().saturating_sub(usize::from(body.height)));
                f.render_widget(
                    Paragraph::new(
                        view.iter()
                            .skip(offset)
                            .take(usize::from(body.height))
                            .cloned()
                            .collect::<Vec<_>>(),
                    ),
                    body,
                );
                f.render_widget(
                    Paragraph::new(
                        error
                            .as_deref()
                            .unwrap_or("↑/↓ scroll   q / Esc / Ctrl-C quit"),
                    ),
                    foot,
                );
            })?;
            redraw = false;
        }
        let wait = if pending {
            Duration::from_millis(100)
        } else {
            next.saturating_duration_since(Instant::now())
        };
        if event::poll(wait)? {
            match event::read()? {
                Event::Key(k) if k.kind != KeyEventKind::Release => match k.code {
                    KeyCode::Esc | KeyCode::Char('q') => break,
                    KeyCode::Char('c') if k.modifiers.contains(KeyModifiers::CONTROL) => break,
                    KeyCode::Up => {
                        offset = offset.saturating_sub(1);
                        redraw = true;
                    }
                    KeyCode::Down => {
                        offset = offset.saturating_add(1);
                        redraw = true;
                    }
                    _ => {}
                },
                Event::Resize(..) => redraw = true,
                _ => {}
            }
        }
    }
    Ok(())
}

pub(crate) fn command(refresh: Duration, json: bool) -> i32 {
    if json {
        return super::workers::emit(
            true,
            read().and_then(|s| {
                serde_json::to_value(s)
                    .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))
            }),
            String::new,
        );
    }
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        eprintln!("error: top requires a terminal for stdin and stdout; use belt --json top for one snapshot");
        return ErrorCode::InvalidInput.exit_code();
    }
    match interactive(refresh) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("error: {}", plain(&e.to_string()));
            ErrorCode::Internal.exit_code()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{RunId, TaskId};
    use serde_json::json;

    #[test]
    fn interval_is_finite_and_bounded() {
        assert_eq!(interval("0.2").unwrap(), Duration::from_millis(200));
        assert_eq!(interval("3600").unwrap(), Duration::from_secs(3600));
        for value in ["0", "-1", "NaN", "inf", "3601", "0.01", "bad"] {
            assert!(interval(value).is_err(), "{value}");
        }
    }

    #[test]
    fn queued_and_held_tasks_are_observed_without_exposing_the_command() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = CoordinatorStore::open(dir.path(), 1).unwrap();
        let run = RunId::generate();
        let queued = TaskId::generate();
        let held = TaskId::generate();
        store.create_run(&run, None, None, &json!({}), 1).unwrap();
        store
            .create_task(
                &queued,
                &run,
                &json!({"commands":[{"node_id":"main","argv":["/usr/bin/portool","exec","-e","never-show-this","--strict","--","/bin/echo","never-show-this"]}]}),
                2,
            )
            .unwrap();
        store
            .create_task(
                &held,
                &run,
                &json!({"ci":{"hold":true,"task":{"task_id":"ci.yml/test"}}}),
                3,
            )
            .unwrap();
        let reader = CoordinatorStore::open_read_only(dir.path()).unwrap();
        let tasks = reader.monitor_tasks().unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].task_id, queued.as_str());
        assert_eq!(tasks[0].label.as_deref(), Some("main"));
        assert_eq!(tasks[0].state, "queued");
        assert_eq!(tasks[0].command.as_deref(), Some("echo [portool]"));
        assert_eq!(tasks[1].state, "held");
        assert_eq!(tasks[1].label.as_deref(), Some("ci.yml/test"));
        assert!(!serde_json::to_string(&tasks)
            .unwrap()
            .contains("never-show-this"));
        store.close_run(&run, 4).unwrap();
        assert!(reader.monitor_tasks().unwrap().is_empty());
    }

    #[test]
    fn uncertain_attempts_and_control_characters_are_displayed_safely() {
        let mut task = MonitorTask {
            task_id: "task".into(),
            run_id: "run".into(),
            command: None,
            label: Some("label\n\x1b[2J".into()),
            worker_id: Some("local".into()),
            state: "active".into(),
            observation: Some("known".into()),
            created_at_ms: 2,
        };
        assert_eq!(section(&task), "Active");
        task.observation = Some("unknown".into());
        assert_eq!(section(&task), "Uncertain");
        let view = Snapshot {
            observed_at_ms: 3,
            workers: BTreeMap::new(),
            tasks: vec![task],
            resident_observed: false,
        };
        let rendered: Vec<_> = lines(&view).iter().map(ToString::to_string).collect();
        assert!(rendered.iter().any(|s| s == "Uncertain (1)"));
        assert!(rendered.iter().any(|s| s.contains("label��[2J")));
        assert!(rendered.iter().all(|s| !s.chars().any(char::is_control)));
    }
}
