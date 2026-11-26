# Changelog

All notable changes to task-traffic-controller (called canirun before 0.5.0) are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/) (pre-1.0: a breaking change bumps
the minor version).

## [Unreleased]

## [0.12.3] - 2026-11-26

### Fixed

- An append that could not get the lock on the history file within a second still wrote, without the lock. Landing between a trim's last look and its rename, that record was lost without a trace; a caller under load could reach that second (one of 600 records missing in `appends_during_trims_are_not_lost`). An append now never writes without the file lock: when it cannot get it in time the record is dropped, counted (`history::dropped_appends`), warned about on stderr and returned as an error, so the command is never held up and no record is lost silently.

## [0.12.2] - 2026-11-26

### Fixed

- A history record could be lost when its append met a trim. `append_record` waits at most a second for the history lock and then appends without it; a trim's rename then dropped a record written to the old file after the trim's last look. Under CPU load an append can miss the lock for that second (seen as 599 of 600 records in `appends_during_trims_are_not_lost`). Now every append takes a shared lock on the history file it writes to and writes only while the path still names that file, and a trim takes an exclusive lock on the old file for its last look and the rename, so such an append waits and goes to the new file. A trim that cannot get that lock within a second leaves the file as it is. An append from a released version takes no file lock and keeps the old exposure.

## [0.12.1] - 2026-11-26

### Fixed

- A gap in the descendant record (failed looks at the process table, see 0.12.0) is now closed only by a look that lists the command alive. Before, any non-empty look closed it, including the final look after the command was reaped, which cannot show the table is complete: a recorded process missed before the gap and again by that look was dropped and the record ended "complete". A gap still open when the command ends now marks the record truncated, as in 0.11.0.
- A recorded process that the look closing a gap does not list is no longer dropped on that look; a later good look has to miss it again.
- When the tracker is dropped without `finish` (the wait for the command failed), a gap still open at its last look is now written to the sidecar as truncated; before, only the in-memory record said so.
- The README and the `GAP_LIMIT` doc said a closed gap keeps the hidden-orphan window open. It does only when the entry is left to the reaper or `resolve`; a normal `managed::finish` releases on the process group, session and marked and recorded processes being gone, as without a gap. Now said so.

### Not changed, on purpose

- `managed::finish` does not keep the entry when a run had a closed gap and a recorded process was dropped after it. With the fixes above such a drop rests on the same evidence as without a gap (two good looks that list the command alive and miss the process). Keeping the entry would hand it to the reaper with the hidden-orphan window open from the command's start, where a hidden orphan that started after the command and leads its own session (the OS starts such processes for the user all the time) keeps it until the window closes, which it then never does: in practice the hold that 0.12.0 removed, for no evidence the release did not already have.

## [0.12.0] - 2026-11-26

### Breaking for library users

- `lineage::step` takes a new `gaps: &mut u32` argument (the failed looks in a row so far; start it at 0).

### Fixed

- One look at the process table that could not be believed (the command not listed although it still runs) no longer marks the descendant record truncated for good. Such a look changes nothing; up to `lineage::GAP_LIMIT` (3) of them in a row followed by a good look are taken as a longer pause between looks, which is what they are: a process that leaves the tree between two looks is missed whatever the pause, and the next good look walks the tree again. Before, a single such look made `managed::finish` refuse with "the descendants of the command could not be recorded" and every later `resolve` and reaper pass end "could not be checked", so the entry was never released. A longer gap, or one still open at the last look, still marks the record truncated, and any gap still leaves the record not covering the command's whole life (the hidden-orphan window stays open).

## [0.11.0] - 2026-11-23

### Breaking for library users

- `Config` has two new public fields, `history_keep_days` and `jobs_log_max_mib`: code that builds a `Config` with a struct literal naming every field no longer compiles (add `..Config::default()`). `jobs::remove_files` now returns `bool`. `lock::sweep_orphan_leases` now returns how many files it removed. `managed::gc_sidecars` no longer removes files that are not sidecars. `Context` has a new public field, `sweep_budget` (build it with `Context::new`).
- `managed::register` no longer does anything else than register; the retention sweep moved to `managed::finish` (below).

### Changed

- `cancel` no longer writes a job's exit record when its runner holds the lease (it was mis-seen as lost or not yet registered): it says `not cancelled, try again` and exits 1.
- Nothing is kept without a rule that removes it, at the moment it stops being needed, with a sweep only as the backstop for crashes. The sweep that `gc` ran now also runs by itself, at most once every 6 hours per state directory, **after** a command has finished and released its place in the queue: at the end of `exec`, of a job (in its runner, after the exit code is written; `submit` and `wait` do not wait for it), of `exec::run_gated` and of `managed::finish`. It only follows a command that ended by itself: not one that a signal ended or that was cancelled, not when a stop is waiting or Ctrl-C was pressed while the command ran, and `managed::finish` runs it only after a command that exited with 0. It takes at most 200 ms (`Context::sweep_budget`), looked at between two items, split so that each of its four stages has 25 ms to look and 25 ms to remove (every sweep gets something done however large a directory is, oldest files first), and a stop that arrives meanwhile ends it at the next item; it never waits for a lock (a busy stage is skipped; the jobs and stray files are still swept when only the ledger lock is busy), holds the ledger lock only to remove what it found before taking it, does at most 500 removals and 4 log cuts a time, and what is left is done 10 minutes later. It never fails the command: a panic in it is caught and said once. `run_gated` takes no signal from the caller's supervisor for it (`Supervisor::take_pending` is not called after the command; the new `Supervisor::stop_pending` only looks). The first command in a new state directory only starts the clock; an empty or unreadable `last-sweep` means "due", and when the stamp cannot be written the sweep still runs and says so. See the Retention section of the README, which says who reads each file again.
- New on stderr: `exec` prints `task-traffic-controller: cleanup: ...`, once per failed step, when the sweep it ran after its command could not do something. `gc` prints the same as `task-traffic-controller: gc: ...`, after doing everything else, and then exits 2 (it used to stop at the first error).
- `wait` removes a job's log and waiter file as soon as it has printed the output, when the job has its exit code and no other `wait` holds it. It does not when `--no-output` was given, when the log could not be read, or when it reports the job as `lost`. A second `wait` on the same job prints the header and exit code only. The descriptor and exit code stay for `jobs_keep_days`.
- Jobs are removed automatically: `jobs_keep_days` (default lowered from 7 to 1, now limited to 3650 at most; 0, valid until 0.10.6, is read as 1 and the command says so once on stderr) after a job finished or was found dead, the sweep removes its files, without `gc`. Never while a `wait` holds the job, even one that waits on another job first.
- A job's exit code is written before its runner leaves the queue (before, after). `wait` and `jobs` therefore no longer see a moment with no queue entry and no exit code, which `wait` could report as `lost`. The run's history record is now written just after the exit code, so it may appear a moment after `wait` returns.
- A job log is bounded while the job runs: over `jobs_log_max_mib` (new, default 16, 1..1048576) the runner cuts it to its last half behind a one-line note, and to the limit when the job ends. The cut is made in the file itself, so the job keeps writing to the same file. Between two looks (200 ms) the log can pass the limit by what the job writes meanwhile, and output written during the instant of a cut can be lost. A log under the limit is unchanged.
- Defaults lowered to what the readers use: `history_keep` 200 to 50 (the learner reads 5), and new `history_keep_days` 30 (the forecast already calls older samples stale; a key unseen that long is dropped, so the number of keys is bounded too). This also shortens how long a measurement id is remembered by `managed::record_measurement`: a measurement sent again after 50 newer records of its key, or after its key was not run for 30 days, is appended again (its first copy is gone by then, so it is never counted twice, but it reads as a recent sample). Raise `history_keep` if that can happen.
- When programs with different configurations share a state directory, each sweep goes by the larger of the retention numbers of the program that runs it and of the configuration file of the state directory (`Paths::config_file`, read at every sweep; the defaults when there is no file or it is not valid, which is said once and fails nothing). So an embedding program with shorter numbers does not remove what the command's `config.toml` keeps, and the other way round. Nothing is recorded: a number lowered in the file takes effect at the next sweep, at once with `gc`.
- `last-sweep` holds only when a sweep last finished and was last started. It is replaced whole (temporary file and rename), also by `gc`, and read again under its lock before a sweep starts, so two processes that find the sweep due at the same moment do not both sweep.
- A job log is never cut under a reader: the runner writes the exit code together with its last cut and cuts nothing afterwards, and the sweep cuts the log of a job that is over only while it holds the job's waiter lock.
- The sweep looks at a lease (which locks it for an instant) last, and only for what age, ledger and exit record already say goes, so a 0.10.x process that is taking a lease is not failed by a sweep that had nothing to do with it.
- The history trim carries over a line that was half written when it read the file (an append without the lock), instead of dropping its first half, and cuts off a line still half written at its last look (nothing could finish it in the new file, and the next record would be lost with it).
- The `.applied` marker of a measurement is removed when the history drops the last record with its id (a marker whose id a kept record still carries stays); a failed measurement settles its `.pending` marker at once; a history left empty and an empty `measurements/` directory are removed. `leases/`, `jobs/` and `managed/` are kept.
- A job whose runner died (no exit file, no ledger entry, lease not held, submitted more than an hour ago) is given a `finished_at`, so its files are removed `jobs_keep_days` later instead of never. It still shows as `lost` until then. Job files whose descriptor is missing or cannot be read are removed an hour after their last change, unless the job has an entry, a held lease or a `wait`.
- A managed sidecar that was never launched, or is not JSON, and has neither a ledger entry nor a held lease is removed an hour after it was written (it was kept for ever). One whose launch was begun (or that another version wrote) and that has no entry and no lease holder is removed `managed_keep_days` after it was last written (it was kept for ever); with an entry it still needs `resolve`, and it stays while the child it records (or its process group's leader) still runs. Files in `managed/` that are not sidecars are no longer removed after `managed_keep_days`: only the tool's own temporary files are, as everywhere else.
- Temporary files are now named `.ttc-tmp.<pid>.<random>` and are removed, an hour old, only when that process is gone (24 hours, 7 days or never before, and by age alone). The `.tmpXXXXXX` files of earlier versions are removed after an hour.
- Lock files are safe to remove: a lease left by a crash is removed only when an hour old and while the remover holds its lock, and every lock taken is checked to be on the file its name points to. `exec` retries for a quarter of a second when its fresh lease is busy (somebody looking at it) instead of failing.
- The history is trimmed without losing records appended meanwhile: `exec` appends under the history lock, and the trim carries over what an earlier version appended without it.
- `Opts::stdout_log` documents that the caller owns the file's size and lifetime.

### Added

- `retention` module: `sweep`, `sweep_if_due`, `Report`, and the pure rules (`due`, `Keep::at_least`, `job_action`, `sidecar_due`, `stale_temp_files`, `orphan_job_ids`, `days_before`, `time_is_up`, `slice_of`). An embedder that writes state without `run_gated` or `managed::finish` calls `retention::sweep_if_due` itself, after its work.
- `Supervisor::stop_pending` (default `false`): whether a stop was asked for, without taking it. `Context::sweep_budget` (default 200 ms).
- `exec::run_gated_with` and `exec::Ending`: the caller is told how the run ends before its ledger entry and lease are released.
- `managed::append_record`, `managed::trim_history_aged`, `history::trim_aged`, `history::trim_collect`, `history::trim_plan`, `jobs::cap_log`, `jobs::cap_running_log`, `jobs::cap_log_unwaited`, `jobs::end`, `jobs::ids_oldest_first`, `managed::sweeps_after`, `history::whole_lines`, `lock::try_acquire`, `lock::remove_if_unheld`, `paths::temp_file_in`, `Paths::sweep_stamp`.

## [0.10.6] - 2026-11-23

### Fixed

- `exec` no longer starts the command with SIGINT and SIGQUIT ignored. While the command runs the wrapper used `SIG_IGN` for them (so the terminal's Ctrl-C does not kill the wrapper), and an ignored disposition survives `exec`, so a command without its own handler could not be interrupted. The wrapper now uses a handler that does nothing, which `exec` resets to the default; the wrapper's own behaviour is unchanged.
- A signal that was ignored when `exec` started (SIGINT/SIGQUIT in a background job of a non-interactive shell, SIGHUP under `nohup`, or SIGTERM) stays ignored in every phase, so the command inherits it exactly as a bare `cmd &` or `nohup cmd` would. The dispositions are read once, before the first handler is installed.
- A stop signal that arrives after the last check while waiting but before the command is spawned now means the command is never started (the run ends as interrupted and its entry is released), instead of starting it and forwarding the signal at once. A signal that arrives after that check, while the command is being spawned, still starts it and is forwarded to the command right after it starts; a SIGINT in that interval is not forwarded (the terminal delivers its own copy to the command's group).

## [0.10.5] - 2026-11-23

### Fixed

- `exec` no longer loses a stop signal: two different signals received before they were consumed are both delivered, and a signal arriving just before the blocking wait is forwarded to the command at once instead of on the next signal. The handler never signals a pid after it was reaped (in-flight counter), keeps `errno`, and an inherited `SIGCHLD = SIG_IGN` no longer auto-reaps the command while its pid is published.

### Added

- `Supervisor::child_started` and `Supervisor::child_exited` (default no-ops) so a supervisor can forward stop signals from its handlers; `take_pending` now returns one signal per call and is called until it returns 0 (at most 64 times per drain).

## [0.10.4] - 2026-11-17

### Changed
- `top`: the `▌` band at the left of a queue row now marks only the running
  rows; a waiting row leaves that column blank, so the running ones stand out.

## [0.10.3] - 2026-11-17

### Changed
- `top`'s `overview` palette is now five clearly different hues (blue, pink,
  cyan, purple, green) instead of eight with light/normal pairs of the same
  hue, and the order a worktree falls back through when its hash slot is
  taken keeps neighbors far apart. With more worktrees than colors, a
  repeated color has its worktree name underlined.

## [0.10.2] - 2026-11-17

### Changed

- **`live::snapshot()` no longer waits 300 ms on every call.** Each process
  keeps its last CPU sample. A call between 200 ms
  (`sysinfo::MINIMUM_CPU_UPDATE_INTERVAL`) and 5 s (`CPU_SAMPLE_MAX_AGE`,
  new) after it samples once without waiting and reports the usage since
  then; a call sooner than that returns the previous figure; the first call
  in a process, and a call after a longer pause, still take two samples
  300 ms apart. A long-lived process that links the crate and asks for a
  forecast or an estimate on every request used to pay the 300 ms each time.
  A command that asks once (`status`, `inspect`) measures as before. Within
  one `exec` or `wait`, the polls after the first now measure over the time
  since the previous poll (they are 500 ms apart) instead of over a fresh
  300 ms, and the "contended" sample right before a start may reuse the
  admission sample taken a moment earlier. `top` likewise shows the usage
  since its previous refresh when that was at most 5 s ago. Callers that arrive while another thread of the
  same process is in the 300 ms wait block until it ends and share its
  figure. Memory and the load average are still read on every call.

## [0.10.1] - 2026-11-17

### Added

- **`Paths::from_env` and `Paths::from_process_env` give a library the CLI's
  scope.** The rule (`XDG_STATE_HOME` or `HOME/.local/state`, and
  `XDG_CONFIG_HOME` or `HOME/.config`, plus `task-traffic-controller`) used to
  live only in the binary; the binary now calls the library function, so a
  program that links the crate shares the CLI's ledger without a second copy
  of the rule. Behavior of the command is unchanged.

## [0.10.0] - 2026-11-16

### Changed

- **`top` shows one table with a progress bar per row, colored by worktree.**
  The running and waiting tables are now one list, numbered from 1 (running
  first, soonest to finish first, then the queue), with a `progress` bar for every row
  (the time run over the expected time fills a gray track in the worktree's
  color; the empty track for a waiting row; `█`/`░` with `NO_COLOR`). The colors no longer mark entries; they mark the git worktree
  a command was queued from, so two parallel runs of one repository from
  different worktrees can be told apart. The row of your own worktree is
  bold, with `(current)` after its branch in a `worktree` column. Held
  entries are one line at the bottom. An empty queue says so and how many
  cores are free; a queue with nobody waiting says what would start at once.
- **`top` has a `focus` and an `overview` color mode (`c` switches).** In
  `focus`, the default inside a git worktree, only the rows of the current
  worktree are colored, in every part of the row (number, project, worktree,
  job, the text after the bar, the bar, its cores slots); the others are dim
  gray with a lighter gray bar fill. `overview`, the default outside a
  worktree, colors each worktree as before, now the whole row. The palette no
  longer has any red or dark blue.
- `exec` records the worktree and branch it was called from in the ledger
  (`worktree`, `branch`; absent in older entries, ignored by older
  versions). They are not part of the history key.

### Fixed

- Rewriting the ledger, a managed sidecar, a job descriptor or the history
  file no longer drops fields this version does not know (a ledger entry's
  `worktree` was lost when an older binary rewrote it). Unknown fields are
  read and written back unchanged; binaries older than this version still
  drop them.

## [0.9.0] - 2026-11-16

### Changed

- **A nested `exec` is now gated.** An `exec` called from inside a gated
  command used to inherit the outer lease and run at once, with no weight
  and no entry. A command that wraps each of its own steps in `exec` (a
  CI runner under `exec`) therefore ran all of them together, each on
  every core. A nested `exec` now registers as a child of the outer entry
  (`parent` in the ledger, `status --json` and `inspect --json`; `[in
  <pid>]` in `status`) and queues against the same budget with its own
  weight, measurement and history.
  - A running entry and what is nested in it take `max(weight, own use +
    what its running children take)`: children use up their parent's
    weight first, and the parent's measurement leaves out its children's
    process trees, so nothing is charged twice.
  - A finished child leaves its CPU time with its parent; the parent's
    run is recorded without it and with the peak memory of its own
    processes, so a command that only starts its steps does not learn
    their weight.
  - The first child of a parent that has no child running is not held by
    the queue or the budget, so a parent waiting for its child cannot
    deadlock and a command that nests one `exec` at a time does not wait
    for the budget. The memory and CPU guards hold it for at most
    `nested_guard_max_wait_secs` (60 s). Each running parent can exceed
    the budget by at most one child.
  - A further child that does not fit starts anyway after
    `nested_sibling_max_wait_secs` (10 min, 0 disables), once every
    running child of the same parent has run that long: a running child
    that waits for a queued one does not wait for ever.
  - Waiting children are ahead of every top-level entry, `--front`
    included. A waiting child whose parent's entry is gone leaves the
    queue without running.
  - A stale lease is gated as a top-level command, as before; a gated
    command's measurement leaves out the tree of every other entry found
    below it. A caller inside a managed run whose supervisor is gone
    still runs under it without an entry, as before.
  - **Ledger format.** While a nested entry exists the ledger is written
    as `version` 3 with `entries_v3`, which binaries older than this one
    refuse (they would write it back without the nesting). Upgrade when
    nothing is queued. A nested entry also writes its `parent` back if it
    finds it missing.
  - `status` warns about entries that name each other as `parent`.

### Added

- `exec --nested admit|inherit` and `TASK_TRAFFIC_CONTROLLER_NESTED`.
  `inherit` is the previous behaviour, for one call or (through the
  environment of the outer command) a whole tree.
- Library: `ledger::Entry::parent`, `admit::QueueEntry::parent`,
  `admit::is_nested`, `exec::Opts::parent`, `exec::NestedMode`,
  `exec::Nesting`, `exec::nesting`, `exec::own_cpu_secs`,
  `managed::try_admit_in`, `ledger::Entry::nested_cpu_secs`,
  `ledger::NESTED_SCHEMA_VERSION`, `Ledger::parent_cycles`,
  `admit::QueueEntry::ran_secs`, `runner::Outcome::own_max_rss_bytes`,
  `runner::Spawn::parent` and `usage::tree_peak_rss`. `usage::tree_cores`
  and `usage::Monitor::start` take more arguments; `usage::Proc` has
  `rss`.
- Configuration: `nested_guard_max_wait_secs` (60) and
  `nested_sibling_max_wait_secs` (600); both are in `admit::Limits`.

## [0.8.2] - 2026-11-14

### Added

- Library callers can use `managed::resolve_checked_typed` and
  `resolve_checked_typed_with` to obtain the exact typed verdict from the
  same locked resolution operation. This avoids parsing diagnostic text or
  performing a second process scan. The existing resolution JSON and API
  remain unchanged; forced resolutions retain the pre-override verdict.

## [0.8.1] - 2026-11-14

### Fixed

- Recovery no longer keeps an unrelated session leader solely because it
  started in the same wall-clock second as a managed command. It is excluded
  only when matching boot and monotonic start evidence proves it predates
  the command. Missing, equal, newer, or conflicting evidence still keeps
  a possible unrecorded descendant.

## [0.8.0] - 2026-11-12

### Added

- `exec --label TEXT` (also `submit` and `estimate`, or
  `TASK_TRAFFIC_CONTROLLER_LABEL`): a name for the run that `status` shows
  instead of the command line. Stored as an optional ledger entry field
  (`label`, omitted when unset), so the ledger stays format v1 and older
  binaries read it; it is not part of the history key. Breaking for code that
  builds `ledger::Entry`, `exec::Opts` or `jobs::Job` with a literal.
- `top`: a full-screen terminal view (ratatui) refreshed every `--interval`
  seconds (default 1) and on resize; `q`, Esc or Ctrl-C quits. It shows the CPU
  budget as a bar with one colored segment per running entry, a running table
  (project, job, `weight/using` cores, progress against the learned wall time),
  a waiting table (why it waits, estimated start) and held entries. Adds the
  `ratatui` dependency. Runs past their expected time are drawn in red.

### Notes

- An older binary that rewrites the ledger drops the `label` of the entries
  it rewrites; upgrade every caller to see labels reliably.

## [0.7.0] - 2026-11-11

This adds a field to `process::ChildIdentity`, which breaks code that builds
that struct with a literal.

### Fixed

- A managed entry whose supervisor was killed is no longer kept as
  `ProbeUnknown` by unrelated processes. With the supervisor dead, any hidden
  process (on macOS: every Apple binary) that started after the command and
  has `launchd` as its parent counted as a possible unrecorded descendant, for
  as long as it lived, and new ones kept arriving: Spotlight workers, other
  `launchd` jobs, orphans of other shells. Such a process is now ruled out when
  its session proves it unrelated: the session is the one of `launchd` itself
  (id 1), or its leader is still running and started before the command. A
  managed child leads its own session and a session cannot be joined, so no
  descendant can be in either. Whether the leader started before the command
  is decided on a clock the wall clock does not move (Linux: clock ticks
  since boot from `/proc/<pid>/stat`; macOS: Mach absolute time from
  `proc_pid_rusage`), recorded for the child at the spawn together with the
  boot id: only a strictly smaller count on the same boot counts. A sidecar
  written by an earlier version has no such record and is kept as before, as
  is everything on a system without such a clock. Everything else is kept as
  before too: a hidden orphan that leads its own session (as a real `setsid` escapee does), one
  whose session leader is gone or started after the command, and every hidden
  orphan of an entry that stopped at its launch intent or whose child was
  recorded without a session. The rule draws one line on purpose: work that a
  command asks `launchd` or an already-running daemon to start (`open`,
  `launchctl submit`, XPC) sits in that service's session and is not counted
  as a descendant.
- `process::signal_if_same` refuses a pid above `i32::MAX` (as a `pid_t` it
  would be negative) before it looks anything up.

### Added

- `managed::hidden_suspects` names the hidden processes that keep an entry
  unresolved (pid, start time, parent, session, and why each cannot be ruled
  out), and `resolve` prints them in `checked`.
- `lineage::standing`, `lineage::Standing`, `lineage::SessionEvidence`,
  `lineage::StartMark`, `lineage::orphan_in_window`,
  `process::session_evidence`, `process::start_mark`, and a provided method
  `ProcessScan::session_evidence`. Its default is "nothing known", which
  never clears a process; only `SystemScan` asks the kernel. An
  implementation whose scan reports this machine's real processes overrides
  it with `process::session_evidence`; one that reports made-up pids must
  not.
- `process::ChildIdentity::start_mark` (optional in the sidecar; absent in
  older ones). `SessionEvidence`, `StartMark`, `Standing` and
  `managed::HiddenSuspect` are `#[non_exhaustive]`; the structs have
  constructors.

## [0.6.0] - 2026-11-10

### Changed

- **Library breaking change:** `exec::Opts` and `runner::Spawn` gained the
  `env` and `stdout_log` fields. They are not `#[non_exhaustive]`, so code that
  builds them with a struct literal must add the fields (or start from
  `Opts::default()`). The CLI is unaffected.

### Added

- `exec::Opts` has `env` (`Some` starts the child from exactly that map, then
  adds the reserved `TASK_TRAFFIC_CONTROLLER_*` variables; `None` inherits) and
  `stdout_log` (stdout and stderr appended to a file, stdin from `/dev/null`;
  `None` inherits). Both reach the child through `managed::run_command`
  (`runner::Spawn` has the same two fields). Neither is part of the history key,
  and the CLI leaves both `None`. `Opts` now derives `Default`.
- `inspect` lists `history_quality` per key (run count, oldest and latest run,
  age of the latest, mean and spread of the wall time). It sits outside
  `history`, so the snapshot digest does not move.
- `forecast` adds a `legacy_clock` assumption when ledger entries are aged from
  their UTC timestamps.

## [0.5.0] - 2026-11-10

### Renamed

- **canirun is now task-traffic-controller.** The crate, the package, the
  command and the library (`task_traffic_controller`) take the new name, and
  every line it prints starts with `task-traffic-controller: `. The `canirun`
  command is gone and nothing in this release is compatible with the old
  name:
  - Environment variables are `TASK_TRAFFIC_CONTROLLER_*` (`LEASE`,
    `LEDGER_LOCK_TIMEOUT_MS`, `MANAGED_ID`, `MANAGED_SCOPE`); the `CANIRUN_*`
    names are not read.
  - State lives in `$XDG_STATE_HOME/task-traffic-controller` (default
    `~/.local/state/task-traffic-controller`) and the config in
    `$XDG_CONFIG_HOME/task-traffic-controller/config.toml`. Nothing is
    carried over from the old directories; remove `~/.local/state/canirun` and
    `~/.config/canirun` by hand when no `canirun` command is still running.
  - To switch: `cargo uninstall canirun`, then
    `cargo install task-traffic-controller`.

### Changed

- A managed command now runs in its own session (`setsid`) and the session id
  is recorded in the sidecar. The reaper, `resolve --force` and `finish`
  count any process of that session as a member, so a process that cleared
  its environment (`env -i`) and was born after the supervisor died or
  between two tracker samples no longer lets the run be freed while it runs.
  The supervisor also stops such leftovers on a normal exit. A session id is
  a pid: it counts only while the recorded child's start time matches the
  leader, or nobody leads it (`reap::session_member`). An unreadable session
  is "could not be checked". Sidecars without a `sid` keep the old rules. A
  daemon that calls `setsid` itself still escapes (documented).
- Adopting a registration after a reboot no longer carries the old boot id
  forward: an unlaunched one is refreshed, one that reached its launch is
  settled as interrupted exactly as the reaper settles it (the id stays used
  up and registering it again answers `AlreadyTerminal`; it is never run
  again), and the boot id is stamped again at the launch intent and at
  the spawn. Otherwise the reaper's reboot shortcut could free the budget of
  a child that runs on the new boot.
- `canirun resolve` also settles a sidecar that is not terminal but has no
  ledger entry (it answered `NoEntry` and left the id blocked).
- `canirun exec` run from inside a managed command whose child has started
  (`CANIRUN_MANAGED_ID` and `CANIRUN_MANAGED_SCOPE` of this state directory)
  runs ungated even after the supervisor and its lease are gone, instead of
  waiting for a budget its own tree holds. The caller must be inside the
  run (its process group, or below its child or a recorded descendant): a
  process that merely inherited the two variables is gated as usual.
- The reaper no longer scans processes under the ledger lock: the scans run
  before the lock is taken, an entry that is kept is assessed again at most
  every 5 seconds, and only a recent release answer that still matches the
  entry and its sidecar is acted on.
- `canirun gc` trims the history under the history lock, removes a
  measurement marker only once its record is gone, and removes the receipts
  of managed commands that ended more than `managed_keep_days` (default 7)
  ago, with no ledger entry. It never removes one that has not ended.
- `canirun resolve --force` releases an entry whose only sign of life is a
  process group that was reused by unrelated processes (the child is gone,
  none carries the run's id, none is a recorded descendant).
- The ledger is written as version 2 whenever the `managed` directory cannot
  be checked, instead of as version 1. A sidecar settled by hand with
  nothing readable left no longer blocks its id with a conflict.
- To go back to an older binary: settle every managed entry (`canirun
  resolve <id>`, or `--force`); the next save then writes version 1 and
  older binaries work again. Until then an older binary exits 2 on every
  command. `canirun status --json` has an additive `ledger_format` field
  (and `status` prints the format when it is 2).

- Library, pre-1.0 breaking: `managed::gc_measurements(ctx)` no longer takes
  a count (a marker lives as long as its record); `managed::trim_history`,
  `managed::gc_sidecars` and `lock::refresh_verdicts` are new; `Context` gained
  `reap_cache` and `reassess_secs` (a struct literal built outside the crate no
  longer compiles; use `Context::new`), and `Config` gained `managed_keep_days`.
- `runner::Outcome` gained `tracked_to_exit_at`, and `tracker::Record` now
  takes a stamp flag and returns whether it persisted; `Tracker::finish`
  takes whether the command was reaped. Pre-1.0: breaking.
- `runner::Spawn` gained a field, `start_tracker`, so a struct literal built
  outside the crate no longer compiles. Pre-1.0: this is a breaking change.

### Fixed

- A failed write of the descendant record is retried every tick until one
  succeeds (it used to be dropped, and the heartbeat clock advanced anyway).
- The hidden-orphan window ends at `recorded_at` + 2 s only when the
  supervisor saw the command exit, took the last look and then confirmed
  the group and anything marked stopped, with nothing left (new sidecar
  field `tracked_to_exit`, stamped after that stop). If the supervisor died mid-run the window is open
  from the child's start, so a hidden process forked late holds the entry
  (`canirun resolve <id> --force` is the way out).
- The last look is told the command was reaped (`wait4`) instead of asking
  the kernel about its pid, which may have been reused and made a normal run
  look failed. A single failed look stays sticky (fail closed).
- A failed look no longer stamps `recorded_at`.
- `recorded_at` is a heartbeat, not the time of the last growth: every good
  look stamps it (written at most about once a second), and the spawn writes
  the first one. That first stamp starts the heartbeat; it is not an upper
  bound on the window (only `tracked_to_exit` ends it).
- A look at the process table that fails, is empty, or does not list a
  command that still runs is persisted at once as a recorder failure
  (`descendants_truncated`), without dropping what was recorded, so a
  supervisor that dies mid-run leaves a hold, not a release.
- More than 4096 live descendants keep the entry held by design (the rest
  are unknown); `canirun resolve <id> --force` is the way out.
- A failed descendant recorder (a panicked or unstarted thread, an
  unreadable running command) is "could not be checked", not "nothing
  recorded". A recorded descendant is dropped only after two looks in a row
  did not find it.
- macOS: a refused `KERN_PROCARGS2` is hidden only for `EINVAL`/`EPERM` on a
  process the kernel confirmed is ours; other errors make the scan unknown.
- A recorder failure is persisted to the sidecar and also stops the
  supervisor that saw it from releasing (`finish` keeps the entry); the
  reaper reads it as "could not be checked" until `resolve --force`.
- A hidden orphan only keeps an entry when it started from the child's start
  (the launch intent when unknown) to two seconds after the last recorder
  write (`recorded_at`), so processes `launchd` starts later no longer hold
  it. A hold that remains is expected; `resolve --force` is the way out.
- A command that ended unreaped (no readable start time) now has its live
  children recorded (the walk is seeded by pid).
- macOS: the command's start time is also read with `proc_pidinfo` when the
  process table does not list it (the short form has no start time, so only
  the full one is used); the retry stays for other platforms.
- macOS: if the kernel cannot tell the owner of a process of yours, a hidden
  process of yours is unreadable, the scan is unknown and the entry is held
  until `resolve --force`.
- A started entry whose child and group are gone is also kept when a hidden
  orphan began after the child (as for a launch intent).
- On macOS an entry can stay held (`ProbeUnknown`) because of a hidden
  process `launchd` started (an Apple binary); `canirun resolve <id> --force`
  releases it.

### Added

- Library: managed commands (`canirun::managed`). `register` takes a
  caller-chosen command id and spec digest (idempotent; a different digest
  is a conflict) and returns a registered receipt only once the ledger
  entry exists. `try_admit`, `run_command`, `finish` and `record_measurement`
  split the admission path into stages. Ownership lives in a sidecar
  (`managed/<id>.json`). A ledger that holds a managed entry is written as
  version 2, with its entries under `entries_v2` and no `entries` key, so an
  older binary refuses to load it instead of reaping the managed entries it
  cannot recognise. Every binary sharing a state directory must be upgraded
  before managed commands are used; a ledger without managed entries keeps
  the old shape and stays readable by older binaries.
- The reaper no longer frees a managed entry just because its owner's lease
  is gone: it keeps the charge until the recorded child and its process
  group are gone, or the boot id changed. An unreadable sidecar is never
  treated as free. `inspect` lists such entries under `managed`. Plain
  `exec` entries are reaped as before.
- Managed commands carry `CANIRUN_MANAGED_ID=<id>` in their environment.
  After the leader exits, and before a managed entry is released, every
  process of the user still carrying the id is stopped (SIGTERM, then
  SIGKILL, only when pid and start time still match) and the entry is kept
  until none is left. This covers descendants that left the process group
  (`setsid`, double fork); one that cleared its environment is not found.
  A scan that cannot read every process never releases anything.
- A launch intent no longer blocks the shared budget forever: with the
  owner's lease gone, 60 seconds since the intent and no marked process, the
  reaper settles it as `Interrupted`. A missing timestamp or an unknown scan
  keeps it.
- `canirun resolve <id> [--json] [--force]` releases an entry whose owner is
  gone after the same checks, and says what it checked. `--force` also
  releases an entry kept only because something could not be checked
  (recorded as `interrupted`, `forced: <reason>`); never one known to be
  running. `status` lists held
  entries and `inspect` adds a `hint` to them.
- `history.jsonl` records written by `managed::record_measurement` carry a
  `measurement_id` (optional; older history still reads). A repeated
  measurement is a duplicate only when its id is in the history, never
  because another record has the same content.
- `canirun inspect --json`: a read-only, versioned snapshot of the ledger,
  limits, live metrics and referenced history, with snapshot and policy
  digests. It never reaps or writes; entries without a held lease are
  flagged `suspect_dead` and still counted.
- `canirun estimate --json -- <command>`: when would this command start and
  finish if queued now? Replays the ledger through `admit` with the same
  limits and queue rules. Unknown values are `null` with a reason, a fallback
  runtime is only ever a `ranking`, and overrunning entries have unknown
  remaining time.
- Library: `inspect`, `forecast` (WorkloadPlan with dependencies, external
  delays, shared setup, an overlay of future demand with de-duplication
  against the ledger, and `coverage`), `estimate::command_plan` and
  `lock::snapshot_read_only`. `Limits` and `LiveSnapshot` are now
  serializable. `error::Error` gained a `Plan` variant.

### Fixed

- Managed commands, hidden environments: on macOS the kernel withholds the
  environment of Apple's own binaries and hardened-runtime apps (about two
  thirds of one measured user's processes), and these were read as "no
  marker", so a `sh -c` command whose child left the process group went
  unseen and its budget was freed while it ran. Such a process is now
  *hidden*, kept apart from absent (and never makes a scan unknown). The
  runner also records the descendants of a running command (pid plus start
  time, read from the process table every 250 ms, at most 4096) in the
  sidecar; a recorded process that still runs keeps the budget and is
  stopped with the marked ones, and a record that overflowed keeps the entry
  as "could not be checked". A launch intent is now also kept while a hidden
  process that began after it is an orphan or the dead supervisor's child.
  Another user's processes are told apart by the kernel's owner (macOS:
  `proc_pidinfo`), not skipped by guesswork.
- Managed commands, review fixes: on macOS the arguments of a process were
  searched for the marker as well as its environment, so an argument could
  hide or fake it; only the environment is read now. The marker also
  carries `CANIRUN_MANAGED_SCOPE` (a hash of the state directory), and only
  a process with the same id and scope is waited for or signalled, so the
  same id in another state directory is left alone. A clear scan has to be
  repeated before it is believed, and an unknown one is retried after the
  signals too. A process whose owner cannot be read is read anyway
  instead of being skipped. Nested managed commands track the innermost one.
- `canirun resolve` on an id the ledger does not have says `no such entry`
  (exit 3, `"found": false` in `--json`) instead of "kept, still charged".
- `canirun gc` removes old `measurements/*.applied` markers (it keeps the
  newest `history_keep`); they used to pile up.
- A job that received SIGTERM twice in a row while queued could die before
  its exit file was written and show as lost. The library no longer restores
  default signal handling on its own; the CLI does it just before it
  re-raises.

## [0.4.2] - 2026-11-05

### Fixed

- **macOS: available memory no longer drops to 0 when much is compressed.**
  The memory guard counted free + inactive + purgeable pages minus the
  pages the compressor occupies, which are none of those. With a few tens
  of GiB compressed, `status` showed `machine 0 KiB available` and every
  command waited for `memory` while anything else ran, even though most
  memory was reclaimable. canirun now reads the page counts itself and no
  longer subtracts the compressor.

## [0.4.1] - 2026-11-04

### Added

- **Short commands no longer wait behind a head that does not fit.** When
  the head of the queue does not fit the free budget, a command behind it
  whose learned wall time is at most `backfill_max_job_secs` (new, default
  300; 0 restores strict FIFO) starts first if it fits. Commands with no
  history never pass anyone, and an entry ahead that will start is never
  overtaken. To keep the head from starving, nothing new passes an entry
  that backfilled commands are holding up once it is `--front` or has
  waited `backfill_max_wait_secs` (new, default 300); a head held up by
  something else, such as a development server that never exits, is
  passed for as long as that lasts. Estimates model the same rule.
- **The waiting line names who holds the budget.** A line waiting on the
  budget ends with `; held by <pid> w2 idle 52:30 <command>, ...` (the
  biggest three running entries). `status` marks running entries `idle`
  (running 5+ minutes, under 0.2 cores) and `[backfill]`, and says what
  a head waiting on the budget needs and how much is free. Idle entries
  are still charged their full weight: charging only measured use would
  under-count work running in a Docker daemon or VM, and an idle server
  can start a rebuild at any time (README, "Idle entries are charged
  their weight").

### Fixed

- **A busy ledger lock no longer fails a queued command.** On a saturated
  machine a holder of `ledger.lock` can stall for seconds, and every
  `exec` waiting on it gave up after 10 s with `ledger: timed out` and
  exit code 2, losing its place in the queue (one test with 40 waiters at
  load ~250 lost 39 of them). `exec` (and the runner behind `submit`) now
  skips that round and tries again, both when it joins the queue and
  while it waits, still answering signals and `cancel` within about a
  second. `status`, `jobs`, `wait`, `cancel` and `submit` read the ledger without
  the lock when it stays busy for a second (saves are atomic renames) and
  leave out entries whose lease is gone instead of failing; `wait` does
  not count such a read toward calling a job lost, since its runner may
  be waiting for the same lock to register. `gc` still fails after 10 s.
- **Fewer ledger writes while waiting.** The head of the queue no longer
  rewrites the ledger on every poll it is held back by the CPU or memory
  guard, and a running command rewrites its `using` value only when it
  changes, so the lock is held less often.
- The `front_jumps_the_queue` test no longer fails on a heavily loaded
  machine: its first job now holds the slot until the test releases it
  instead of sleeping for a fixed second.

## [0.4.0] - 2026-10-18

### Added

- **Running commands are charged what they actually use.** Each running
  `canirun` samples its command's process tree once a second (macOS and
  Linux) and writes the cores used (peak of the last three samples) to
  its ledger entry. Admission charges a running entry `max(weight,
  round(using))`, so a command declared at weight 1 or 2 that runs five
  cores of workers no longer lets the next one start on top of it.
  `status` shows it as `w2 (using 5.1)` and counts it in `budget X/Y
  used`; the ledger field is `using`. Without a measurement (first second,
  or a platform that cannot be read) the declared weight is used. The
  command itself is never slowed or stopped.

- **Test runners and mutation tools start heavier.** A command nothing
  has been learned about starts at `test_initial_weight` (new, default 4)
  when it is a test runner and `mutation_initial_weight` (new, default 6)
  when it is mutation testing, instead of `default_weight`. A pattern's
  `initial_weight` or `weight` still wins.
- **`--front` waits briefly on an overloaded machine.** When the load
  average is at least `front_overload_factor` (new, default 2.0) x
  `budget`, a `--front` head waits for at most
  `front_overload_max_wait_secs` (new, default 30) before starting, with
  a progress line `overloaded: load N, limit M`. Previously `--front`
  skipped every CPU check, so a pre-push hook could start on a machine
  already running at many times its cores. Set the factor to 0 for the
  old behaviour. The same cap bounds how long measured use beyond the
  running entries' weights can hold a `--front` entry; after it, only
  declared weights count for that entry.

### Changed

- **Wrapped commands learn from their earlier runs.** `sh -c` / `bash -c`
  / `zsh -c` lines and scripts under a temporary directory used to get a
  new history key on every run (different redirections, temp paths, a
  new scratch file name), so they ran at `default_weight` forever. They
  are now keyed by the first heavy command inside them (`npm run <script>`,
  `npx`, `node`, `vitest`, `jest`, `stryker`, `cargo test`, `dart test`,
  `flutter test`, ...), with redirections dropped and temporary paths
  masked; a temporary script is read (first 8 KiB) for the same purpose,
  or keyed by its file name. Plain commands keep exactly the key they had
  before. History recorded for a wrapped command under 0.3 is not
  migrated: the new key starts learning from its first run (or shares
  what the plain command already learned). `--key` still wins.

### Fixed

- **A history test no longer flakes when its run ends under the 1 s learning
  floor**: it now burns 1.2 s of user + sys CPU time per process.

## [0.3.0] - 2026-10-17

### Added

- **A hint for a command passed as one quoted argument.** When
  `canirun exec -- "npm run x"` (or `submit`) fails because no program
  named `npm run x` exists, the error suggests `-- npm run x`, or
  `sh -c '...'` for shell syntax.

### Changed

- **Fewer progress lines when stderr is not a terminal.** A waiting line
  is printed only when the wait reason (queue / budget / memory / cpu) or
  the queue position changes, and otherwise every
  `progress_interval_secs_non_tty` (new, default 60). On a terminal the
  `progress_interval_secs` cadence is unchanged. Long waits under an agent
  no longer fill its tool output with identical lines.

### Fixed

- **Back-to-back `submit`s keep their order.** `submit` returned before
  its detached runner had joined the queue, so a second `submit` could
  start first, and its estimate (and `jobs`) ignored the job just
  submitted. `submit` now returns once the runner is in the queue
  (typically under 0.1 s) and computes the estimate from the real queue.
  If the runner has not joined within 5 s, `submit` prints a warning and
  returns the id anyway.

## [0.2.1] - 2026-10-16

Feedback from the first day of use on a machine where most load did not go
through canirun yet.

### Changed

- **`--front` entries skip the CPU guard** (the memory guard still applies).
  A human waiting on a hook must not be held by load canirun does not
  manage.
- **The CPU guard gives up after `cpu_guard_max_wait_secs`** (default 120):
  once the head has waited that long it is admitted on the budget and
  memory checks alone.
- **Runs that start on a saturated machine are recorded as `contended`**
  (one CPU sample right before the start) and are left out of weight and
  memory learning whenever a calm run exists. Under contention
  `cpu / wall` under-reports parallelism, which would admit too much.
  `history` marks such runs.
- **Keys include the subdirectory within the repository**
  (`git rev-parse --show-prefix`), so `api/` and `client/` of one monorepo
  learn separately. Root-level keys hash exactly as before, so their
  history carries over. `status` / `jobs` / `history` show `repo/subdir`.

## [0.2.0] - 2026-10-16

### Added

- `canirun submit [--detach] [--weight N] [--front] [--key K] -- <command>`:
  start a job in a detached runner and print its id. The runner follows
  the same admission, measurement and liveness path as `exec`; its merged
  log, descriptor and exit code live under `jobs/` in the state directory.
- `canirun wait [--no-output] <id>...`: block until the jobs finish, print
  each log in the order given, exit with the first non-zero code. `wait`
  holds a shared lock per job; a job with no holder is cancelled by its
  runner once `jobs_orphan_grace_secs` (default 30) have passed since
  submit, unless submitted with `--detach`.
- `canirun jobs [--json]` and `canirun cancel <id>...`.
- Rough completion estimates from learned wall times and the current
  queue: shown by `submit`, in the `exec` progress line, and in `status` and
  `jobs`. Unknown commands count as 60 s and are marked with `?`.
- `jobs_keep_days` (default 7): `gc` removes older finished job files.
- A signal that arrives between the last queue check and admission, or
  between handler installation and spawn, is no longer lost.

## [0.1.0] - 2026-10-16

First release.

### Added

- `canirun exec [--weight N] [--front] [--key K] -- <command>`: wait for
  budget, run the command in canirun's process group with inherited stdio,
  forward SIGTERM/SIGHUP, mirror the exit code or signal death, and record
  CPU time, wall time and peak RSS via `wait4`.
- Learned weights: ceiling of the highest CPU parallelism over the last
  five usable runs of the same `(repository, argv)`; peak RSS learned the
  same way. `config.toml` patterns provide `weight` (fixed), `min_weight`
  and `initial_weight` hints.
- Live guards: available memory must cover predicted RSS plus
  `memory_reserve_mib`, and global CPU usage must be under
  `cpu_threshold_percent`, before the head of the queue starts. Skipped
  when nothing managed is running.
- FIFO queue with head reservation; `--front` for interactive callers.
- Crash-safe liveness via per-entry `flock` lease files.
- Nested `exec` inherits a live `CANIRUN_LEASE`; a stale one is gated.
- `canirun status [--json]`, `canirun history`, `canirun config [--path]`,
  `canirun gc`.
