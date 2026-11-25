# task-traffic-controller

A local admission controller for heavy commands.

A command asks before it starts, waits until the machine has room, runs,
and reports what it cost so the next run of the same command knows its
weight. No daemon, no process supervision, one JSON ledger per user.

```sh
task-traffic-controller exec -- npm test                 # waits for budget, then runs
task-traffic-controller exec --front -- npm run lint     # a human is waiting: jump the queue
task-traffic-controller status                           # who is running, who is waiting, why, and when
task-traffic-controller top                              # full-screen live view (q to quit)
task-traffic-controller history                          # what each command measured, next weight

id=$(task-traffic-controller submit -- npm run test:e2e) # async: returns a job id and a rough ETA
task-traffic-controller wait "$id"                       # keeps the job alive, prints its log, mirrors its exit
```

## What & why

When several agent sessions each reproduce CI locally at the same time —
lint, unit, build, e2e, mutation tests, times ten — a 12-core laptop ends
up running 30–50 heavy processes. Everything slows down and the total
takes longer than running them one after another. The same happens when a
`pre-push` hook starts mutation tests while the machine is already
saturated.

`task-traffic-controller` is the traffic controller:

- **One budget.** A number of "slots" (default: logical CPU count) shared
  by every `task-traffic-controller exec` of the same OS user. The sum of running weights
  never exceeds it.
- **Learned weights.** Every run is measured (`wait4` rusage: CPU time,
  wall time, peak RSS). A command's weight is the ceiling of the highest
  CPU-parallelism it showed in its last five usable runs, scoped to the
  repository and subdirectory it ran in. Runs that started on a saturated
  machine are marked `contended` and ignored while calmer runs exist,
  because contention makes parallelism read low. Nothing to hand-tune;
  `config.toml` holds only hints and exceptions.
- **Live brakes.** Before the head of the queue starts, task-traffic-controller also
  checks the machine as it is right now: available memory must cover the
  command's learned peak RSS plus a reserve, and global CPU usage must be
  under a threshold. These delay the *next* start; a running command is
  never paused or killed. `--front` entries skip the CPU brake, and any
  head gives up on it after `cpu_guard_max_wait_secs`, so load that does
  not go through task-traffic-controller cannot hold the queue indefinitely. On a
  machine that is badly overloaded (load average at least
  `front_overload_factor` x budget) a `--front` entry still waits, for at
  most `front_overload_max_wait_secs` (30 s). Likewise, measured use
  beyond the running entries' weights holds a `--front` entry for at most
  that long; after that only declared weights count for it. (The load
  average is the trigger because measured cores cannot exceed the core
  count, so "twice the budget" is only visible as runnable processes.)
- **Measured, not just declared.** While a command runs, task-traffic-controller samples
  the CPU its whole process tree uses. A command declared (or learned) at
  weight 2 that is using 5 cores takes 5 from the budget until it slows
  down, so the next start waits instead of piling on. The running
  command itself is never slowed or stopped.
- **Crash-safe queue.** Each waiting or running entry holds an `flock` on
  its own lease file. A Ctrl-C'd, SIGKILLed, or crashed `task-traffic-controller` — waiting
  or running — vanishes from the ledger on the next poll. No cleanup step,
  no daemon, no pid heuristics.
- **FIFO with reservation, plus backfill.** The head of the queue
  reserves until it fits. While it does not fit, a command behind it that
  is known to be short (learned wall time at most
  `backfill_max_job_secs`, default 5 min) may start first if it fits, so
  one heavy head no longer stops everything behind it. A heavy unit is
  still not starved: once a head would fit but for backfilled commands,
  nothing new passes it after it has waited `backfill_max_wait_secs`
  (default 5 min), or at once when it is `--front`. `--front` lets a human
  at the keyboard jump ahead.
- **Nesting is gated too.** A gated command that itself calls
  `task-traffic-controller exec` (a CI runner that wraps each of its
  steps, a `make` whose recipes do) gets an entry of its own for each
  call, queued against the same budget as a child of the outer entry. A
  child uses up its parent's weight first and is charged only for what
  goes beyond it, and what it runs is measured (and learned) on the
  child, not on the parent as well. It cannot deadlock: the first child
  of a parent that has none running is not held by the budget, and no
  child waits for the budget for ever (see "Nested commands").
- **Async jobs that die with their waiter.** `submit` returns a job id as
  soon as the job is queued; `wait` collects the log and exit code later. A job nobody is
  waiting on is cancelled after a grace period, so a crashed caller never
  leaves a test suite burning CPU. `--detach` opts out.
- **Rough ETAs.** From learned wall times and the current queue, every
  waiting entry gets a "start in / done in" estimate on `submit`, in the
  progress line, and in `status` / `jobs`.

## Install

```sh
cargo install task-traffic-controller
```

Coming from `canirun` (before 0.5.0)? Run `cargo uninstall canirun` first.
The old state and config directories are not read; delete
`~/.local/state/canirun` and `~/.config/canirun` when you are done with them.

Requires macOS or Linux. Prebuilt binaries for macOS (Apple Silicon /
Intel) and Linux (x86_64 / arm64) are attached to each
[release](https://github.com/t09tanaka/task-traffic-controller/releases/latest).

## Usage

### `task-traffic-controller exec`

```
task-traffic-controller exec [--weight N] [--front] [--key K] [--label TEXT] [--nested admit|inherit] -- <command> [args...]
```

Waits until admitted, then runs `<command>` with inherited stdin/stdout/
stderr in task-traffic-controller's own process group, with `TASK_TRAFFIC_CONTROLLER_LEASE=<id>` in its
environment. The exit code is the child's; a child killed by a signal
makes task-traffic-controller die from the same signal. The command runs directly
(`execvp` semantics), not through a shell — pass `sh -c '...'` yourself
if you need one.

- `--weight N` overrides the derived weight for this run (still measured).
- `--front` puts the entry ahead of all normal entries. Several `--front`
  entries are FIFO among themselves.
- `--key K` replaces the argv string as the history key, for commands
  whose arguments carry temp paths or timestamps. It wins over the
  automatic keys below.
- `--label TEXT` (or `TASK_TRAFFIC_CONTROLLER_LABEL`; the flag wins) names
  the run. `status` and `top` show it instead of the command line. It is
  stored in the ledger as an optional field and is not part of the history
  key. Also accepted by `submit` and `estimate`.
- `--nested admit|inherit` (or `TASK_TRAFFIC_CONTROLLER_NESTED`; the flag
  wins) says what happens when this `exec` is called from inside another
  gated command. `admit`, the default, queues it as a child of the outer
  entry. `inherit` runs it at once under the outer entry, with no entry,
  weight or history of its own, as every nested `exec` did before 0.9.
  See "Nested commands".

Wrapped commands are keyed by the heavy command inside them, so a
one-off shell line still learns from earlier runs:

- `sh -c` / `bash -c` / `zsh -c` / `dash -c '<line>'` (also behind `env`,
  `time`, `nice`, `timeout` and `VAR=value`, and through nested `-c`
  strings): the first command in the line that runs `npm run` / `npm test`
  / `npm exec` (and the pnpm / yarn / bun equivalents), `npx`, `node`,
  `stryker`, `vitest`, `jest`, `mocha`, `playwright`, `pytest`,
  `cargo test|build|check|clippy|bench|nextest|mutants|llvm-cov`,
  `go test|build|vet`, `dart test|run|analyze` or
  `flutter test|build|analyze`. Redirections are dropped, so
  `cd api && npm run test 2>&1 | tail -5` is keyed as `npm run test` and
  shares its history with `task-traffic-controller exec -- npm run test`.
- A script under a temporary directory (`/tmp`, `/private/tmp`,
  `/var/folders`, `$TMPDIR`, or any `scratchpad` directory), run directly
  or as `bash /tmp/x.sh`: its first 8 KiB are read and the same rule
  applies; if nothing is found (or the file cannot be read), it is keyed
  by its file name and arguments.

Temporary paths among the kept arguments become `<tmp>`. A line with no
heavy command keeps the full argv string as its key. `history` shows the
derived key as `key: ...` under the command.

While waiting, a progress line goes to stderr every few seconds:

```
task-traffic-controller: waiting 0:42 (queue) — running 3 (10/12 used), ahead of you 2
task-traffic-controller: waiting 52:10 (budget) — running 2 (4/12 used); held by 48211 w2 idle 52:30 api: make api, 48190 w2 idle 51:02 app: flutter run
task-traffic-controller: waiting 1:07 (memory 1.1 GiB available, need 3.4 GiB) — running 2 (6/12 used)
task-traffic-controller: waiting 0:12 (cpu 93% busy, threshold 85%) — running 2 (6/12 used)
```

A line waiting on the budget names the running entries holding it
(biggest first, at most three): pid, weight, `idle` or `using N`, and how
long they have run, so a forgotten server is easy to find and stop.

When stderr is not a terminal (a log file, an agent's tool output), a line
is printed only when the reason or the queue position changes, plus one
every `progress_interval_secs_non_tty` (default 60) as a heartbeat, so a
long wait does not fill the output with identical lines.

If the program is not found and the command after `--` is a single
argument containing spaces (`-- "npm run x"`), task-traffic-controller adds a hint to pass
the words separately (`-- npm run x`) or use `sh -c '...'`.

Nothing is written to stdout by task-traffic-controller itself.

### `task-traffic-controller top [--interval SECS]`

A full-screen live view of the queue (alternate screen, raw mode), refreshed
every `--interval` seconds (default 1) and redrawn on resize. Switch the colors with `c`. Quit with `q`,
Esc or Ctrl-C; the terminal is restored on every exit path, a panic included.
It needs a terminal (use `status` to pipe), and only reads and reaps like
`status`.

- **cores 12/12 in use · 0 free** (the box under the title): a bar of
  `budget` slots. Each running entry's charged weight is a segment in its
  worktree's color, led by the `#N` of its row in the table; free slots are
  dim. On the right of the box's top edge: the machine's CPU busy share and
  available memory.
- **queue**: one row per entry, numbered `#1`, `#2`, ... down the screen:
  the running ones first, soonest to finish first (those with no learned
  time last), then the waiting ones in queue order. Columns:
  - a `▌` in the worktree's color on a running row (a waiting row leaves
    it blank), and the number;
  - `project` (repo name, plus `/subdir`);
  - `worktree`: the branch (the short commit hash when detached), in the
    worktree's color, cut in the middle with `…` when it is long (never under 4 characters),
    and ` (current)` after the one `top` was started in, which is never
    cut. That worktree's rows are
    bold. A repository with only one worktree in the queue, and entries
    queued by an older version (which recorded none), show `-` and a gray
    band;
  - `job`: the `--label`, else the script's file name for `bash ... step.sh`
    commands, else the command line; with `idle`, `[backfill]` or `[front]`
    after it (`z`, `b`, `f` on a narrow terminal); `idle` marks a run
    (5:00+) using under 0.2 cores. The cores of each row are in the box on
    top, not in the table;
  - `progress`: a bar for how far each running row is, and the text after it.

  **Colors** have two modes, shown at the top (`focus` / `overview`) and
  switched with `c`. In `focus`, the default when `top` is started inside a
  git worktree, only the rows of that worktree are colored (one fixed green):
  the whole row, `#N` to the text after the bar, and its slots in the cores
  box. Every other row is dim gray, its bar filled with a lighter gray. In
  `overview`, the default outside a worktree, each worktree of a repository
  with several gets its own color (a lone worktree stays gray), again for the
  whole row. The palette is five clearly different hues (blue, pink, cyan,
  purple, green) with no red or yellow; red is only for an overrun. With more
  worktrees than colors a repeated color is shown with its worktree name
  underlined.
  The bar of a waiting row is the empty track in both.

  `exec` records the worktree (`git rev-parse --show-toplevel`) and branch
  it was called from, so parallel runs of one repository from different
  worktrees can be told apart (the history key still collapses them into
  one). The color is picked from a hash of the worktree's path, moving on to
  the next free color of the palette when another worktree of the same
  repository has it; it is not stored.

  On a terminal 80 columns wide or more the bar is never narrower than 20
  characters (10 on a narrower one). To keep it, a narrow terminal gives up,
  in this order: the reason for waiting after the bar (`starts ~1:35?`
  stays), then the project, worktree and job columns are squeezed to 20, 20
  and 24 characters (the middle of the name goes), the words `idle` and
  `[backfill]` (they shrink to `z` and `b`; `[front]` to `f`), and only then
  the three names are cut further (8 characters at the least; the worktree
  column keeps room for 4 of the branch and ` (current)`). No column is
  dropped; the bar and the text after it always stay.
  - `progress`: one bar per row, all the same width, which takes
    whatever the other columns leave. The track is a pale gray background;
    the time run over the expected wall time fills it from the left with the
    worktree's color (a darker gray for a lone worktree), in steps of 1/8 of a
    cell; past the expected time it stays full. A run with no learned wall
    time has the track only. A waiting row shows the empty track (0%); its place in
    the queue is its number. With `NO_COLOR` set (or `TERM=dumb`) the bar is
    drawn with `█` and `░` instead.
  - After the bar: `62%  left 0:47` / `over 0:20` / `ran 0:12` (no learned
    time) for a running entry; `starts ~0:47` (`starts ?` with no estimate,
    `~0:47?` when it is rough because an entry it depends on has no
    history) and why it waits (`needs 6, 3 free`, `behind 2`, ...; left out
    when there is no room) for a waiting one.
  - When something runs but nobody waits, a dim line under the table says
    what would start right away (`No one waiting · a command needing ≤8
    cores starts right away`, or that the next command waits for cores when
    none are free).
- **An empty queue** says `Nothing running or waiting` and, dim, how many
  cores are free (`A new command starts right away · 12 cores free`).
- **held, owner gone**: one line at the bottom, shown only when there is
  such an entry.

### `task-traffic-controller status`

```
budget 10/12 used
machine 31.2 GiB available, cpu 71% busy

running (3)
    48211  w2  (using 0.0) idle    52:30  api: make api
    48190  w4  (using 4.1)          1:03  webapp: npm run test:e2e
    48260  w2  (using 0.9)          0:20  [backfill] api: npm run test:unit
  (idle: running 5:00+ and using under 0.2 cores; a server or watch mode holds its weight until it exits)

waiting (2)
    48302  w9      0:41  [front] api: npm run test:gates
    48310  w1      0:40  webapp: npm run lint

the head needs w9, 4 of 12 free; held by the running entries above
```

`(using N)` is the cores the running command's process tree used
recently (see "How it works"); `budget X/Y used` counts each running
entry at its weight or at that measurement, whichever is more. `idle`
marks an entry that has run for 5 minutes and is using almost nothing:
usually a development server or a watch mode, which holds its weight
until it exits (see "What not to wrap"). `[backfill]` marks an entry
that started ahead of a head that did not fit. `[in 48190]` marks an
entry nested in the running entry with that pid; it is counted inside
that entry's share of the budget first (see "Nested commands"), and in
`--json` and `inspect --json` it carries the outer entry's id as
`parent`. `--json`
dumps the ledger. Dead entries are reaped first, so the view is accurate
without a daemon.

### `task-traffic-controller history [--key K] [--json] [--limit N] [-- <command>]`

Measured runs per command key, and the weight and memory that will be
used next:

```
webapp: npm run test:e2e
  next: weight 4 (Learned), memory 3.4 GiB
  10-16 21:04  wall   184.2s  cpu   611.0s  x3.3  rss   3.4 GiB  exit 0   w4
  10-16 20:12  wall   190.7s  cpu   640.1s  x3.4  rss   3.2 GiB  exit 0   w2
```

### `task-traffic-controller submit` / `wait` / `jobs` / `cancel`

```
task-traffic-controller submit [--detach] [--weight N] [--front] [--key K] [--label TEXT] -- <command> [args...]
task-traffic-controller wait [--no-output] <job-id>...
task-traffic-controller jobs [--json]
task-traffic-controller cancel <job-id>...
```

`submit` writes a job descriptor, starts a detached runner (own session,
stdin from `/dev/null`, stdout and stderr merged into the job log,
environment and cwd inherited), waits until the runner has joined the
queue (so back-to-back submits keep their order; at most 5 s, after which
it warns and returns anyway) and prints the job id on stdout, with a
rough estimate on stderr:

```
$ id=$(task-traffic-controller submit -- npm run test:e2e)
task-traffic-controller: job 3f9c… start in ~1:10, done in ~4:20
```

`wait` blocks until every named job has finished, prints a header and the
log for each in the order given, and exits with the first non-zero code
(`143` for a cancelled job). While `wait` runs it holds a shared lock on
the job; **a job with no holder is cancelled once `jobs_orphan_grace_secs`
(default 30) have passed since submit**, so the usual `submit` → `wait`
pair keeps it alive and a caller that dies takes its jobs down with it.
`--detach` keeps a job running regardless. `jobs` lists every job with
its state (`queued` / `running` / `done N` / `orphaned N` / `lost`) and
ETA; `cancel` sends SIGTERM to the runner, or marks a not-yet-started job
cancelled (a job whose runner is alive but not in the queue yet is left
alone: `cancel` says it did not cancel it and exits 1; try again).

The runner is the same admission path as `exec`: weights, guards, history
and liveness all apply. Job files live in `jobs/` under the state
directory. `wait` removes the job's log as soon as it has printed it (unless
another `wait` is on the same job, `--no-output` was given, or the job is
reported as `lost`); the descriptor and exit code stay for `jobs` and a
repeated `wait` until `jobs_keep_days` (see [Retention](#retention)). A job
log longer than `jobs_log_max_mib` (default 16) is cut to its last part,
while the job runs and when it ends, with a note on its first line saying
how much was cut; a normal job's output reaches `wait` whole.

### `task-traffic-controller inspect --json` and `task-traffic-controller estimate --json -- <command>`

Both only read; they never register an entry, take a lease, reap a dead
entry or write history (`status` reaps, `inspect` does not).

`inspect` prints the ledger (a `suspect_dead` flag on entries whose lease is
not held, which are kept and counted as busy), the limits, the live
machine metrics and the history the ledger entries refer to, with a
`snapshot_digest` and a `policy_digest`.

`estimate` predicts when the command would start and finish if it were
queued now, by replaying the ledger through the same admission function
`exec` uses. Times are milliseconds from now; a value that cannot be
computed is `null` with the reason in `null_reasons`. With no usable history
`runtime_source` is `fallback`: the finish time is `null` and only
`ranking` carries a number (a 60 s default, good for ordering candidate
machines, not an ETA). A running entry past its recorded runtime has an
unknown remaining time, not zero. Live memory and CPU guards are read once;
what depends on them is marked `conditional`.

The same machinery is available to Rust callers as `task_traffic_controller::inspect` and
`task_traffic_controller::forecast` (a multi-node `WorkloadPlan` with dependencies, external
delays and shared setup, plus an overlay of demands the ledger does not
show yet). The crate is 0.x and that API is not stable.

### `task-traffic-controller resolve <id> [--json] [--force]`

A managed command (one driven through `task_traffic_controller::managed` by another
program) needs every `task-traffic-controller` that shares the state directory to be
upgraded: a ledger that holds a managed entry is written in a new shape
(`entries_v2`), and an older binary refuses to load it rather than reap
entries it cannot recognise (every command then exits 2). To go back to an
older binary, settle every managed entry (`task-traffic-controller resolve <id>`, or
`--force`); the next save writes the old shape (`version` 1) and older
binaries work again. `task-traffic-controller status` shows the format (`ledger_format` in
`--json`). It keeps its budget while its supervisor is gone, until nothing it
started is still running. `status` lists such entries as `held, owner gone`,
and `inspect` marks them `quarantined` with a `hint`. Most are released by
themselves once their processes stop. Two kinds are not: an entry whose
sidecar cannot be read, and a launch intent whose timestamp is missing. For
those, `task-traffic-controller resolve <id>` runs the same checks on demand: the owner's
lease must be gone, and the child, its process group, every process of
yours carrying `TASK_TRAFFIC_CONTROLLER_MANAGED_ID=<id>` (and this state directory's
`TASK_TRAFFIC_CONTROLLER_MANAGED_SCOPE`) and every recorded descendant must be gone. It prints the entry's state, what was checked and the result, and
records the outcome as `interrupted`. An id whose sidecar is not terminal
but has no ledger entry (stranded, for example by an older binary) is settled
the same way. Exit 0 means released, 1 means kept
(still charged), 2 an error, 3 that the ledger has no entry with that id
(it prints `no such entry`, and `--json` has `"found": false` and
`"verdict": "NoEntry"`). It never kills anything.

A launch intent (the supervisor died around the spawn, so it is unknown
whether a child exists) is released by the reaper itself once the lease is
gone, at least 60 seconds have passed since the intent and no process
carries the id. An unknown scan, or no timestamp, keeps it: time alone never
releases it. `resolve` skips the 60 seconds.

`resolve --force` is for an entry kept only because something **could not be
checked**: a process that could not be read, a launch intent that a hidden
process may belong to, or a record of descendants that did not fit. It
releases the entry anyway and records `interrupted` with `forced: <reason>`
(`--json` has `"forced": true`). It never releases what is known to be
running: a held lease, a running child, a marked process or a recorded
descendant keeps the entry (exit 1), with or without `--force`. A live
process group alone does not count as running when the group number was
reused, and only positive evidence shows that: a process whose pid is the
recorded group number is alive and started at a different time than the
recorded child (a new leader), the child is gone, and no process carries
the run's id or is a recorded descendant. Then `--force` releases the entry
(`forced: the recorded process group number now belongs to a different
process group`). A live group with no such leader, or one whose start time
cannot be compared, may be the original group (members without the marker)
and stays held. Exit codes
are the same (0 released, 1 kept, 2 error, 3 no such entry).

How descendants are found, and what this cannot see:

- **Marker.** A managed command carries `TASK_TRAFFIC_CONTROLLER_MANAGED_ID` and
  `TASK_TRAFFIC_CONTROLLER_MANAGED_SCOPE` (the first 16 hex digits of the SHA-256 of the
  state directory's canonical path), which its descendants inherit. Two
  state directories that use the same command id do not see, hold or stop
  each other's processes. Only the innermost managed command is tracked:
  one started inside another overwrites the outer id and scope.
- **Hidden environments.** On macOS the kernel withholds the environment of
  Apple's own binaries (`/bin/sh`, `/bin/bash`, `/usr/bin/env`, `perl`,
  `sleep`, ...) and of hardened-runtime apps. Such a process is *hidden*:
  neither marked nor proven unmarked. A hidden process alone never makes a
  scan "unknown" (on one measured Mac about two thirds of the user's
  processes are hidden; that would block every release), and an empty
  environment cannot be told from a hidden one, so it counts as hidden too.
  Processes of other users are not scanned. On Linux every process of the
  user is readable, except one that made itself non-dumpable (an agent or
  sandboxed helper), which is read as unmarked.
- **Descendants are recorded while the command runs.** The runner reads the
  process table every 250 ms and records every process below the command
  (pid plus start time, following parent links) in the sidecar's
  `descendants` (at most 4096; more are not recorded and
  `descendants_truncated` is set; processes that exited are dropped from the
  record). A recorded process that still runs keeps the budget, however it
  left the process group (`setsid`, double fork), whatever its environment
  hides, and it is stopped like a marked one (SIGTERM, then SIGKILL, after
  checking pid and start time again). If descendants were dropped, a clear
  scan cannot be believed and the entry is kept as "could not be checked".
  A look that cannot be believed (the table does not list the command
  although it still runs) changes nothing in the record. Up to 3 such looks
  in a row are a longer pause between two good looks: the tracker only sees
  processes at its looks, a process that forks and leaves the tree between
  two looks is not recorded however far apart they are, and the next good
  look walks the whole tree again from the command and every recorded
  process, so such a gap loses nothing a pause of the same length would not.
  Only a look that lists the command alive ends a gap, and a recorded
  process it does not list is dropped only if a later good look misses it
  too. A longer gap, or one still open at the last look (including when the
  command ended during the gap), marks the record truncated for good. After
  a gap that was closed the record does not count as covering the
  command's whole life: if the entry is later left to the reaper or
  `resolve`, the window for hidden orphans stays open. A normal end of the
  run is not affected: the entry is released once the process group, the
  session and the marked and recorded processes are gone, as without a gap.
- **A launch intent and hidden processes.** A launch intent (the supervisor
  died around the spawn) is released by the reaper once the lease is gone,
  60 seconds have passed, no marked or recorded process runs, *and* no
  hidden process that started after the intent has `launchd`/`init` or the
  dead supervisor as its parent. Any such process keeps the entry (it may be
  the child); `resolve --force` overrides that. No timestamp keeps it.
  `resolve` skips the 60 seconds.
- A scan that finds nothing is repeated once and believed only if it finds
  nothing again; an unknown one is repeated a few times first.
- On macOS a process of yours whose environment cannot be read at all is
  hidden. The owner of a process comes from the kernel (`proc_pidinfo`).

What is not found, and what follows:

- A descendant that forks and loses its parent within one sampling interval
  (250 ms), before it was recorded, and whose environment is hidden or
  cleared: not found. The marker finds it if its environment is readable.
- A hidden descendant whose parent was never recorded and which is an orphan
  of something other than `launchd` (for example reparented to a user
  subreaper): not found once the command's own record is gone.
- A hidden orphan that is *not* a descendant is ruled out when its session
  proves it: the session is `launchd`'s own (id 1), or its leader still runs
  and started before the command (a managed command leads its own session,
  and a session cannot be joined). One that leads its own session, or whose
  session leader is gone or started after the command, cannot be told from
  a real escapee and keeps the entry; `resolve` names it in `checked`.
  "Started before the command" is not read off the wall clock, which can be
  set back: the command's start is recorded as clock ticks since boot (Linux)
  or Mach absolute time (macOS) together with the boot id, and compared with
  the leader's. A leader is older only on the same boot and with a strictly
  smaller count; an entry recorded by an earlier version has no such count
  and is never released this way.
- The session rule draws a line at `launchd` / `init`: a process counts as a
  descendant only if it was forked below the command. Work the command
  *asks* `launchd` or an already-running daemon to start (`open`,
  `launchctl submit`, an XPC request; on Linux `systemd-run`, D-Bus
  activation) is not counted, and does not keep the entry.
- A descendant that runs as another user (it ran a setuid program) is not in
  the scan at all, hidden or not: not found once its parent is gone.
- A hidden descendant that starts after the wall clock was set back to
  before the command's start looks older than the command: not found.
- A descendant that cleared its environment (`env -i ...`) loses the
  marker; it is still found if it was recorded while its parent lived, and
  in any case while it stays in the run's **session**.
- **Session.** A managed command is started in its own session (`setsid`;
  its session id and process group id are the child's pid, recorded in the
  sidecar as `sid`). `env -i`, a double fork, a process born after the
  supervisor died or between two samples, and a job-control shell that
  moves its jobs to new process groups all stay in it, so the reaper and
  `resolve --force` see them as members and keep the entry. A session id is
  a pid, so it counts only while it is still the child's session: a process
  that leads that number now and started at a different time than the
  recorded child is a different session (reuse), and its members are not
  counted. If a process's session cannot be read the scan is unknown, as
  for an unreadable environment. A sidecar written before this has no `sid`
  and is judged as before. A consequence: the command has no controlling
  terminal of its own.
- **Left the session.** A daemon that calls `setsid` deliberately gets out
  of the run's session (`setpgid` cannot: POSIX does not let a process move
  into a group of another session), and
  with an empty environment, born after the tracker last looked, nothing
  finds it: the entry can be freed while it runs. This is the residual
  limitation. Another: the session check lists processes through the same
  process table as everything else, so a process that table does not list is
  missed; the marker and the recorded descendants still catch it in the
  common cases.
- The process group check does not compare start times, so a group number
  that was reused by an unrelated group keeps an entry held for as long as
  that group lives. That errs on the side of keeping the budget; the way out
  is `task-traffic-controller resolve <id> --force`, which releases it only when a new
  leader (the process whose pid is the group number, started at a different
  time than the recorded child) proves the number was reused, nothing in the
  group carries the run's id or is a recorded descendant, and the child is
  gone. Without that proof the group may be the original one (a member that
  cleared its environment, or was born after tracking stopped) and stays.
- A scan that could not read some process of the user is unknown and
  releases nothing, until `resolve --force`.
- **A Mac entry that stays held.** On macOS a process that `launchd`
  started (an Apple binary whose environment is hidden) can be mistaken for
  the child of a launch intent or a started command: the reaper then
  answers "could not be checked" (`ProbeUnknown`) and the entry stays held.
  Only a hidden process that started in a window counts: from the child's
  start (the launch intent when that is unknown) to two seconds after the
  last time the recorder wrote (`recorded_at` in the sidecar). That end
  applies only when the supervisor saw the command exit, took the last
  look and confirmed everything it left stopped (`tracked_to_exit` in the
  sidecar, stamped after that stop); with no record, or when the
  supervisor died mid-run (the command may have forked later), the window
  has no end. Such a hold is expected, not a bug,
  and the way out is `task-traffic-controller resolve <id> --force`, after checking that
  nothing of the command still runs.
- Recording the descendants that fails is written to the sidecar too
  (`descendants_truncated`), and the supervisor that saw it fail does not
  release either: the entry stays held until `resolve --force`.
- If the kernel cannot tell the owner of a process of yours (macOS
  `proc_pidinfo` refused both ways), a process whose environment is hidden
  is unreadable, the scan is unknown, and the entry is held. The way out
  is the same: `task-traffic-controller resolve <id> --force`.
- Recording the descendants that fails (the thread panics or cannot start,
  or the command cannot be read while it runs) counts as "could not be
  checked", never as "no descendants". A recorded descendant is dropped only
  after two looks in a row did not find it.
  A look at the process table that fails, comes back empty, or does not list
  a command that still runs is a recorder failure too: it is written to the
  sidecar at once, during the run, so a supervisor that dies mid-run leaves
  a hold, not a release. A write that fails is retried every tick until one
  succeeds. `recorded_at` is a heartbeat: every good look stamps it (written
  about once a second; a failed look never stamps it), and the spawn writes
  the first.
- A single failed look is sticky on purpose: even a transient read failure
  of the process table marks the record truncated, and the run is held until
  `task-traffic-controller resolve <id> --force`. The last look is told the command was
  reaped, so it does not ask the kernel about a pid that may be reused.
- **More than 4096 live descendants.** Only 4096 are recorded; past that the
  rest are unknown, so the record is marked truncated and the entry stays
  held after the command ends, by design. The way out is
  `task-traffic-controller resolve <id> --force`, after checking that nothing of the command
  still runs.
- On macOS a process whose environment the kernel refuses with anything but
  `EINVAL`/`EPERM`, or whose owner the kernel did not confirm, is
  unreadable and makes the scan unknown.

### `task-traffic-controller config [--path]` and `task-traffic-controller gc`

`config` prints the effective configuration (defaults merged with the
file). `gc` runs the [retention](#retention) sweep now and prints what it
did. The same sweep runs by itself; `gc` is for running it on demand and
seeing its errors (it exits 2 when a step failed, after doing the rest).

## Configuration

`~/.config/task-traffic-controller/config.toml` (or `$XDG_CONFIG_HOME/task-traffic-controller/config.toml`).
Absent file means all defaults.

```toml
budget = 12                 # default: logical CPU count
default_weight = 2          # used when nothing is known about a command
test_initial_weight = 4     # ...unless it is a test runner
mutation_initial_weight = 6 # ...or a mutation-testing tool
history_keep = 50           # records kept per command key
history_keep_days = 30      # a key not run for this long is dropped (1..3650)
progress_interval_secs = 5  # between progress lines on a terminal
progress_interval_secs_non_tty = 60 # heartbeat when stderr is not a terminal
memory_reserve_mib = 2048   # keep this much available before admitting
cpu_threshold_percent = 85  # hold the next start while CPU is busier
cpu_guard_max_wait_secs = 120 # ...but never for longer than this
front_overload_factor = 2.0 # --front waits while load average >= budget x this (0: never)
front_overload_max_wait_secs = 30 # ...but never for longer than this
backfill_max_job_secs = 300 # a command learned to take at most this may pass a head that does not fit (0: never)
backfill_max_wait_secs = 300 # ...until that head, held up by such commands, has waited this long
nested_guard_max_wait_secs = 60 # the first nested exec of a command is held by the memory/CPU guards for at most this long
nested_sibling_max_wait_secs = 600 # a nested exec that does not fit starts anyway after this, one per period (0: never)
jobs_orphan_grace_secs = 30 # a job nobody `wait`s on is cancelled after this
jobs_keep_days = 1          # an uncollected job's files are removed this long after it finished or was found dead (1..3650; 0 is read as 1)
jobs_log_max_mib = 16       # a job log is cut to its last part beyond this, while it runs too (1..1048576)
managed_keep_days = 7       # receipts of managed commands are removed this long after they ended (1..3650)

[[pattern]]                 # glob over the full argv string, first match wins
match = "*stryker*"
min_weight = 6              # learned weight may not drop below this

[[pattern]]
match = "*test:e2e*"
min_weight = 4

[[pattern]]
match = "*eslint*"
weight = 1                  # fixed; measured but never learned
```

Weight priority: `--weight` > pattern `weight` > learned (then
`min_weight`) > pattern `initial_weight` > the kind's initial weight >
`default_weight`. Floor 1, ceiling `budget`.

The kind comes from the key (the command found inside a wrapper, see
`exec`): mutation testing (`stryker`, `cargo mutants`, any word containing
`mutant` / `mutation`, such as `npm run test:mutation`) starts at
`mutation_initial_weight`; a test runner (`npm test`, `npm run test` /
`test:*`, `vitest`, `jest`, `mocha`, `playwright`, `pytest`, `cargo
test` / `nextest`, `go test`, `dart test`, `flutter test`) at
`test_initial_weight`. Once a run has been measured, the learned weight
takes over.

## How it works

State lives in `~/.local/state/task-traffic-controller/` (or `$XDG_STATE_HOME/task-traffic-controller/`):

- `ledger.json` — the queue: id, pid, state, weight, repo, worktree and branch, argv, times.
  Writes are temp-file + rename under `ledger.lock`. A waiting `exec` that
  cannot get the lock skips that poll and tries again; `status`, `jobs`,
  `wait`, `cancel` and `submit` read the file without the lock when it
  stays busy for a second.
- `leases/<id>.lock` — one file per entry; the owning `task-traffic-controller` holds an
  exclusive `flock` on it from registration until exit. **Liveness is
  "this flock cannot be taken."** Anyone holding the ledger lock probes
  each lease non-blockingly and drops the entries whose lock it obtains.
- `history.jsonl` — measurements, appended to by every run and trimmed by
  the retention sweep (see [Retention](#retention)).

Admission is a pure function of the ledger plus a live snapshot: the
candidate must be the head of the queue (`--front` entries first, then
by enqueue order), `used + weight ≤ budget`, and — when something managed
is already running — available memory must cover its predicted RSS plus
the reserve and CPU usage must be under the threshold.

A candidate behind the head may start first (backfill) when all of these
hold, and the budget and guards then apply to it as to a head:

- its learned wall time is known and at most `backfill_max_job_secs`
  (a command with no history never passes anyone);
- every entry ahead of it either does not fit the budget or cannot start
  before it anyway (it is not the head and not short itself), so entries
  that will start are never overtaken and short commands keep their order;
- no entry ahead is being held up only by backfilled entries (it would
  fit without them) while being `--front` or having waited
  `backfill_max_wait_secs`. Such an entry gets the budget as soon as the
  short commands already running end; the waiting line says `queue; the
  entry ahead has waited long enough, nothing more passes it`.

A head held up by something else — a long build, or a server that never
exits — is passed for as long as that lasts, because backfill does not
delay it. When nothing
managed is running the live guards are skipped, so a machine kept busy by
processes task-traffic-controller does not manage cannot hold the queue forever.

While a command runs, a background thread in its `task-traffic-controller` samples the
CPU used by the command and all of its descendants once a second and
writes the peak of the last three samples to the ledger entry as
`using`. Admission charges each running entry `max(weight,
round(using))`, so a command that uses far more than its weight holds the
next start; a new or idle one is charged its weight. When the tree cannot
be measured, the declared weight is used. Measuring does not touch the
command. CPU spent in a Docker daemon or VM is not part of the tree
(see "Known blind spot").

Estimates are a plain simulation of that queue against the budget (it
asks the same admission function who starts at each step, so backfill is
included) using each entry's learned wall time (max of its last five usable runs; 60 s and
a trailing `?` when unknown). The live guards are not modelled, so an
estimate is a lower bound.

Weights are learned per `(repository, subdirectory, key)`, where the key
is the argv string or the command found inside a wrapper (see `exec`). The
repository is the main worktree (`git rev-parse --git-common-dir`), so
every linked worktree of a project shares one history; the subdirectory
(`git rev-parse --show-prefix`) keeps `api/` and `client/` of one monorepo
apart.

### Nested commands

A command that runs under `exec` has `TASK_TRAFFIC_CONTROLLER_LEASE` in
its environment. An `exec` that finds a live lease there registers as a
*child* of that entry and is admitted like any other command, with its
own weight, measurement and history. Three rules make that safe.

- **A child takes from its parent's share first.** A running entry and
  everything nested in it take `max(weight, own use + what its running
  children take)` from the budget. With an outer command at weight 4,
  two children at weight 2 cost nothing more; a third makes the group
  take 6. An outer command that only starts and waits for its steps can
  declare `--weight 1`: while a step runs, the group takes what the step
  takes.
- **Nothing is measured twice.** The sampler of the outer entry leaves
  out the process trees of its children, which their own entries
  measure.
- **Nothing is learned twice.** A finished child leaves its CPU time
  with its parent, and the parent's run is recorded without it (and with
  the peak memory of its own processes only). An outer command that only
  starts and waits for its steps keeps a small learned weight instead of
  growing to the sum of its steps.
- **The first child is not held by the budget.** A command that waits
  for its child cannot finish, so the budget it holds would never come
  back. When no child of a parent is running, the first one in the queue
  starts although the budget is full, and ahead of everything else. The
  memory and CPU guards still hold it, so that several parents each
  starting one more command do not exhaust the machine, but for at most
  `nested_guard_max_wait_secs` (60 s). A command that runs its nested
  commands one at a time therefore never waits for the budget, and each
  running parent can take the budget over by at most one child.
- **Further children wait until they fit, but not for ever.** A running
  child may itself be waiting for one that is queued (a server and the
  client that stops it). A child that has waited
  `nested_sibling_max_wait_secs` (10 min; 0 turns this off) starts
  although the budget is full, once every running child of the same
  parent has also run that long, so at most one more starts per period.
- Waiting children are ahead of every top-level entry, `--front`
  included, since the budget their parent holds comes back only after
  they have run.
- **Stopping the outer command drops what has not started.** A waiting
  child whose parent's entry is gone (the outer command was stopped, or
  ended without waiting for it) leaves the queue without running. A child
  that already runs carries on as a top-level entry.

An `exec` whose lease is stale (the outer command is gone) is gated as a
top-level command; if it still runs below a gated command, that
command's measurement leaves its process tree out too. A caller inside a
managed run whose supervisor has died still runs under that run without
an entry, as before.

`--nested inherit`, or `TASK_TRAFFIC_CONTROLLER_NESTED=inherit` in the
environment of the outer command, restores the old behaviour for
everything below it: the nested command runs at once and is charged to
the outer entry through its measurement. Use it where a nested command
must never wait: a build whose own parallel jobs keep the CPU above the
threshold would otherwise hold each nested call for up to
`nested_guard_max_wait_secs`, and a nested long-lived server holds its
weight against its siblings like any other entry.

**Upgrading.** While a nested entry exists the ledger is written in a
shape (`version` 3, `entries_v3`) that binaries from before nested
admission (0.8 and older) refuse, because they would write it back
without the nesting and charge commands twice. `status` says so while it
lasts. An older `exec` that is waiting or starting at that moment fails
with a ledger error; one that is already running its command finishes
it, and its entry is reaped afterwards. Upgrade when nothing is queued,
or let running commands finish before the first nested one starts.

Known limits:

- A nested step with no history starts at the default weight, so on its
  first runs several can start before their measurements arrive.
- The ETA and `top` do not model nesting: estimates treat nested entries
  as ordinary ones, and `top` does not mark them.
- A managed command (the library's `register` path) that has nested
  entries is measured by its caller, which does not get the nested CPU
  time taken off.
- Entries that name each other as `parent` (only possible by editing the
  ledger) are not counted in the budget; `status` warns about them.

### Idle entries are charged their weight

A running entry measured at nearly zero cores (a development server, a
watch mode) still takes its full weight from the budget. Charging it
only what it measures would be wrong in two common cases: an integration
suite whose work runs in a Docker daemon or VM measures near zero in its
own process tree (see below) while the machine is busy, and an idle
server can start a full rebuild at any moment. Either way the budget
would admit more than the machine can take. Instead, `status` marks such
entries `idle` and the waiting line names them, so they can be stopped
or run without task-traffic-controller.

### Known blind spot

`wait4` rusage covers the child process tree only. Work pushed into a
Docker daemon or VM by integration suites is invisible, so such commands
learn a parallelism near 1. Use `min_weight` on a pattern for them; the
live CPU and memory guards still see the real load.

## Integrating

### Git hooks

Keep working on machines without task-traffic-controller:

```sh
# .husky/pre-push
if command -v task-traffic-controller >/dev/null 2>&1; then
  task-traffic-controller exec --front -- npm test
else
  npm test
fi
```

### What not to wrap

Only commands that burn CPU for tens of seconds or more: test suites,
builds, mutation testing, integration and e2e runs. Do not wrap `sleep`,
`git`, dependency installs, formatters, or one-second checks: each wrapped
command takes a slot (default weight 2) and queues behind heavier work.
Do not wrap development servers, watch modes or anything else that does
not exit on its own: it holds its weight for as long as it runs, and a
heavy head that does not fit next to it waits until it is stopped.

### Agent / CI-runner scripts

Prefix every unit with `task-traffic-controller exec --`, or `submit` the independent
units and `wait` for them all in one call. A unit may wait minutes
before it starts; with `exec` run it in the background or under a
monitor rather than inside a foreground step with a short hard timeout.
Keep the `wait` alive for as long as you care about the result: if your
process dies, so do its jobs.

### Sharing the CLI's scope from a library

A program that links the crate and must use the same ledger as the command on
the same host asks for the CLI's scope instead of repeating the rule:

```rust
let paths = task_traffic_controller::Paths::from_process_env();
// or, testable without touching the process environment:
let paths = task_traffic_controller::Paths::from_env(home, xdg_state_home, xdg_config_home);
```

State is `$XDG_STATE_HOME/task-traffic-controller` (else
`$HOME/.local/state/task-traffic-controller`), config is
`$XDG_CONFIG_HOME/task-traffic-controller/config.toml` (else
`$HOME/.config/...`). An empty XDG value counts as unset, a relative one is
used as given, and without `HOME` the root is `/`.

### Reading the machine from a long-lived process

The machine snapshot behind the live guards (`live::snapshot()`, which
`forecast` and admission read through `SystemLive`) waits 300 ms between two
CPU samples only when the process has no CPU sample from the last 5 seconds.
A long-lived process that asks more often than that gets the usage since its
previous sample without waiting, and the previous figure itself when it asks
again within 200 ms. Memory and the load average are read on every call.

## Retention

Everything is removed at the moment nothing reads it any more; a sweep is
only the backstop for what a crash left behind and for what only time makes
old. The sweep runs by itself, at most once every 6 hours per state
directory, **after** a command has finished and released its place in the
queue: at the end of `exec`, at the end of a job (in its detached runner,
after the exit code is written, so neither `submit` nor `wait` waits for
it), and for a program that embeds the library at the end of
`exec::run_gated` and `managed::finish`. Nothing is swept before a command
starts or while it waits.

The automatic sweep never delays or fails anything:

- it only follows a command that ended by itself. Not one that a signal
  ended or that was cancelled, not when a stop signal is waiting or Ctrl-C
  was pressed while the command ran, and, for `managed::finish`, only after
  a command that exited with 0 (the caller of a failed or cancelled one
  gets its answer at once);
- it takes at most 200 ms by the clock (`Context::sweep_budget` for a
  program that embeds the library), and a stop that arrives while it runs
  ends it too. Both are looked at between two items, an item being one
  file looked at or removed, one log cut (a copy of up to
  `jobs_log_max_mib`), one rewrite of the history or one listing of a
  directory, so it ends at most one item later. The time is split in eight
  parts of 25 ms: each of the four stages (ledger and receipts, history,
  jobs, stray files) has one for looking and one for removing, counted
  from its first look and its first removal. So a directory with more in
  it than can be read in that time does not keep the sweep from removing
  what it found, or the other stages from running: every sweep gets
  something done, the files written longest ago are looked at first, and
  what is left is done 10 minutes later;
- it takes no lock it would have to wait for. A stage whose lock is busy is
  skipped, the sweep is not recorded as done, and it is tried again 10
  minutes later. The jobs and the stray files are swept all the same: they
  only need to read the ledger. The ledger lock is held only to remove what
  was found before it was taken (listing `managed/` and `leases/` and
  reading the files is done without it);
- it removes or marks at most 500 files and cuts at most 4 job logs a time
  (the rest 10 minutes later), and looks at no process other than with
  signal 0;
- a step that fails is reported on stderr once, as
  `task-traffic-controller: cleanup: ...`, by the `exec` that ran the sweep,
  and the other steps still run. A job's runner does not print them (its
  stderr is the job's log); `gc` does. `managed::finish` prints them on
  stderr too, each different line once in a process. A panic in the sweep
  is caught, said the same way, and changes neither the command's exit
  status nor what `run_gated` or `managed::finish` return.

`gc` applies the same rules on demand, waits for the locks, has no limit
and no time budget, and exits 2 after printing every error.

When the sweep is due is read from `last-sweep` (about a hundred bytes of
JSON: when a sweep last finished and when one was last started, nothing
else). Only a missing file starts the 6-hour clock (the first command in a
new state directory). A file that is empty or cannot be read means "due",
and when it cannot be written the sweep still runs, after every command,
and says so: a full disk is when it is needed. The file is replaced whole
(written beside it and renamed), by the automatic sweep and by `gc` alike,
so it is never read half written. One process sweeps at a time: it locks
the file, reads it again (another process may just have swept) and writes
its start into it before it does anything.

| what | who reads it again | removed | at most |
|---|---|---|---|
| `leases/<id>.lock` | the queue's liveness test, while the command lives | by its command when it ends; by whoever reaps a dead entry; by the sweep an hour after a crash left one with no entry, and only while the sweep holds its lock | one per live entry |
| `jobs/<id>.json`, `.exit` | `jobs`, `cancel`, and a repeated `wait` (the exit code lives nowhere else) | by the sweep, `jobs_keep_days` (1) after the job finished or was found dead, and never while a `wait` holds the job: a day is how long a result can wait for its reader | a few hundred bytes per job in a day |
| `jobs/<id>.log` | `wait`, which prints it | by `wait`, the moment it has printed the output of a job that has its exit code (a job reported as `lost` keeps its log); else with the job | `jobs_log_max_mib` (16 MiB, which `wait` reads into memory) per job, while it runs too |
| `jobs/<id>.waiter` | `wait`, while it runs | by the last `wait` when it has reported; else with the job | one per job |
| `history.jsonl` | the weight learner (the newest 5 usable records of a key), `estimate`, `inspect`, `history` | each record beyond `history_keep` (50) per key, and every key not run for `history_keep_days` (30, the age at which the forecast already calls a sample stale), by the sweep; the file itself when nothing is left | 50 records for each key run in 30 days |
| `measurements/<id>.applied` | `record_measurement`, to refuse a replay while a record with that id exists | at the moment the history drops the last record with that id | one per kept record |
| `measurements/<id>.pending` | nobody (a dead attempt's) | at once when the append fails; the sweep for a crash. It becomes `.applied` if its record exists | none |
| `managed/<id>.json` | a repeated registration of the same command id finds its outcome here | `managed_keep_days` (7) after the command ended, never while it has an entry or its owner holds the lease; one that was never launched or is not JSON, and has neither, an hour after it was written (a crash between writing it and saving the ledger). One whose launch was begun, or that another version wrote, and that has neither, `managed_keep_days` after it was last written: with an entry it is held for `resolve`, without one nothing charges for it or settles it any more, unless the child it records (or its process group's leader) still runs | per command in 7 days |
| temporary files (`.ttc-tmp.<pid>.*`) | nobody but the process that is writing one | by the sweep, when an hour old **and** process `<pid>` is gone (a writer stopped for longer keeps its file). The `.tmpXXXXXX` files of earlier versions name no process and go by age alone | none |
| job files with no readable descriptor | nobody | by the sweep, when every file of that id is an hour old and the job has no entry, no held lease and no `wait` | none |
| empty `measurements/` | nobody | when the last file in it goes | none |
| `ledger.json`, `ledger.lock`, `history.lock`, `last-sweep`, and the directories `leases/`, `jobs/` and `managed/` | every command | never | four small files, three directories |

Why the last row is fixed: several versions of this tool may run on one
machine against the same state directory. The versions before this one
create a directory and then open a file in it, and open a lock file and then
lock it, without looking again. A directory or a lock that vanishes in
between makes them fail, or leaves them holding a lock nobody else can see.
So `leases/`, `jobs/` and `managed/` are never removed (`measurements/` is
only made and removed under the lock that every version takes first),
and a lock file is removed only by its owner or while the remover holds its
lock; this version also checks, after taking any lock, that the name still
points to the file it locked, and starts again if not. What remains with an
older version: a lease file an hour old with no entry (a crash left it) may
be removed in the instant between an older binary's open and its lock, when
that binary registers the same command id again; the older binary then
holds a lease nobody sees and its entry can be released early. Ids are
random except for managed commands that are registered again.

Looking at a lease means locking it for an instant, and an older binary
that is taking that lease at the same instant fails with "lease already
held" (this version asks again for a quarter of a second). So the sweep
looks at a lease last, and only for what age, ledger and exit record
already say goes: a job with no exit file and no entry that was submitted
more than an hour ago, a managed sidecar with no entry that is past its
time, a lease file an hour old with no entry. What remains: an older
binary that takes the lease of exactly such an id in that instant (a
managed command registered again after `managed_keep_days`, or a job whose
runner needed more than an hour to start).

How a job is found dead: it has no exit file, no ledger entry, and nobody
holds its lease (its runner holds the lease from registration until after
the exit file is written, so this is the same test the queue uses for a
command), and it was submitted more than an hour ago. The sweep gives it
the finish time it never got (`finished_at`, now), so `jobs_keep_days` later
its files go. Until then `jobs` still shows it as `lost`.

A job's log is bounded while the job runs. The runner looks at its size
five times a second and, when it is over `jobs_log_max_mib`, cuts it to its
last half behind a one-line note; at the end of the job it is cut to the
limit. The cut is made in the file itself (the part to keep is copied aside,
the file is truncated, the part is written back), so the job and anything it
started keep writing to the same file, at its new end, and the space is free
at once. What this costs: between two looks the log can pass the limit by
what the job writes in 200 ms; the output written during the instant of a
cut can be lost or land before the kept part; and a process that outlives
its job and still holds the log open goes on writing to it. After `wait`
has printed and removed the log, such a process writes to a file with no
name, emptied at that moment: only what it writes from then on takes space,
until it exits. Before that, the sweep cuts the log of a job that is over
every time it runs.

A cut never happens under a reader. The runner makes its last cut and
writes the exit code as one step, and cuts nothing after it, so whoever
sees the exit code reads a log its runner has left alone. The sweep cuts
the log of a job that is over only while it holds that job's waiter lock
(taken without waiting), which every `wait`, of this and of earlier
versions, holds from its start until it has reported. What remains: a
`wait` that reports a job as `lost` while its runner is in fact alive and
slow (three looks with no entry and no exit code) may print a log the
runner is cutting; it does not remove it.

The cut relies on the log being open for appending. On Linux a process
that opens `/dev/stderr`, `/dev/stdout` or `/proc/self/fd/N` again gets a
new descriptor on the same file without the append mode (and empties the
log if it opens it for writing without it). After a cut such a process
writes at its old offset, which leaves a hole of zeros before its output:
the file takes no more disk space than what was written since, but its
size and what `wait` prints include the hole. Processes that write to the
descriptors they were given, which is nearly all, are not affected; macOS
is not (there `/dev/stderr` duplicates the descriptor).

When `wait` returns, the job has its exit code. Two things follow a moment
later, because the exit code is written first: the run's record in the
history (`history`, `estimate`), and the release of the job's place in the
queue (its entry still counts against the budget for that moment, so a
command started right after `wait` may wait for it).

Programs with different configurations may share a state directory (this
command, and a program that embeds the library with its own `Config`).
Neither removes what the other keeps: for each of `history_keep`,
`history_keep_days`, `jobs_keep_days`, `jobs_log_max_mib` and
`managed_keep_days`, a sweep uses the larger of the number of the program
that runs it and the number in the configuration file of the state
directory, which every sweep reads (`config.toml`; for an embedding program
the `config_file` of the `Paths` it was given, which must be the file this
command reads for that state directory, as `Paths::from_env` gives it).
With no such file the defaults stand in for it, and with one that is not
valid too, which is said once on stderr and fails nothing. Nothing is
recorded anywhere: a number changed in the file counts from the next sweep
on, so lowering one takes effect at once (`gc` frees the space right away)
unless the program that sweeps has a larger number of its own. An
embedding program therefore cannot keep less than the file (or the
defaults) says; to keep less, lower the number in the file.

A measurement is applied once for as long as a record with its id is in the
history. With `history_keep = 50`, a measurement sent again after 50 newer
records of the same key (or after the key was not run for 30 days) is
appended again; its first copy is gone by then, so no reader ever sees both
(the learner reads the newest 5), but it is read as a recent sample. A
program that may send again that late raises `history_keep`.

Files the caller owns are not ours. `Opts::stdout_log` (the library's way
to send a command's output to a file) is appended to by the library and
never truncated, rotated or removed by it: its size and lifetime belong to
the caller. A `history.jsonl` placed elsewhere with
`Paths::with_history_file` is trimmed like any other; in its directory the
sweep removes only this version's own temporary files (`.ttc-tmp.<pid>.*`,
an hour old, process gone). Files in the state directory with names that
are not ours are never removed (before, anything in `managed/` that was not
a sidecar went after `managed_keep_days`).

A program that embeds the library gets the sweep from `run_gated` and
`managed::finish`. One that writes state in other ways (only
`managed::record_measurement`, for example) calls
`retention::sweep_if_due(&ctx, &cfg)` from its own entry points, after its
work; `retention::sweep` runs it unconditionally, as `gc` does.

## Platform support

macOS and Linux. `flock`, `wait4`, process groups and signal forwarding
are POSIX; `ru_maxrss` units (bytes on macOS, KiB on Linux) are
normalised. The ledger scope is one OS user + `XDG_STATE_HOME`; task-traffic-controller
does not coordinate across users, containers with a different `HOME`, or
machines.

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
./scripts/coverage-gate.sh      # 100% lines on admit / weight / key / eta
cargo install cargo-schemata --locked --version 0.1.0   # once
cargo schemata --file src/admit.rs --file src/weight.rs --file src/key.rs --file src/eta.rs --file src/forecast.rs --file src/reap.rs --file src/receipt.rs --file src/marker.rs --file src/lineage.rs -- --lib
```

On a Homebrew Rust toolchain (no `llvm-tools-preview`), point `cargo llvm-cov`
at Homebrew's LLVM: `LLVM_PROFDATA=/opt/homebrew/opt/llvm/bin/llvm-profdata
LLVM_COV=/opt/homebrew/opt/llvm/bin/llvm-cov ./scripts/coverage-gate.sh`.

The admission decision, weight derivation, command key, estimate and
forecast simulation are pure modules with 100% line coverage and a zero-survivor mutation gate in CI.
`tests/cli.rs` drives the binary end-to-end with isolated XDG directories.

## License

MIT
