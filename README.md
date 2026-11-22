# local-ci

Run the GitHub Actions jobs your branch would trigger, on your machine, and get
a short report back.

```
local-ci run          # what a pull request of these changes would trigger
local-ci run --all    # every check a pull request can trigger
```

`local-ci` reads `.github/workflows/` (or the directories
[`.local-ci.toml`](#local-citoml) names), works out which workflows a pull request
from this branch would start (triggers, `branches`, `paths` / `paths-ignore`),
and runs their jobs in a throwaway `git worktree` of the commit. It prints one
line per job and, for a failure, the end of the failing step's log. Full logs
and a JSON result are kept on disk.

It is meant for the moment before you push, and for coding agents that need to
know "would CI pass?" without reading workflow files and logs themselves.

With [`examples/demo`](examples/demo) and a change that breaks one test shard:

```
$ local-ci run --no-status
local-ci 0.1.0 · 350924e vs main · 1 file changed · triggered by the changes · logs ~/Library/Caches/local-ci/runs/demo-2daacfc9/1793539365-350924e
setup  ok  1 step 0s
PASS   ci.yml › lint  0s
PASS   ci.yml › discover  0s
PASS   ci.yml › test (1)  0s
FAIL   ci.yml › test (2) › Unit tests (shard 2) (exit 1)  0s
PASS   ci.yml › integration  3s
SKIP   deploy (needs failed)

── ci.yml › test (2) › Unit tests (shard 2) (exit 1)
   expected 5, got 4
   log: ~/Library/Caches/local-ci/runs/demo-2daacfc9/1793539365-350924e/ci-test-2/03-unit-tests-shard-2.log

result: failure · pass 4 / fail 1 / error 0 / skip 1 · 3s
status: not posted (--no-status)
```

It is not a GitHub Actions runner. Jobs run directly on your machine (not in
Ubuntu containers), with the tools you have installed, so `runs-on: ubuntu-latest`
runs on a Mac as it is: a green job is a pass on the OS of your machine, not
on Linux (see [Jobs for another OS](#jobs-for-another-os-runs-on)). What cannot
be reproduced locally is skipped and listed under `not covered` instead of being
reported as a pass.

## Install

```
cargo install local-ci
```

Or download a binary from the [releases](https://github.com/t09tanaka/local-ci/releases).

## Commands

| Command | What it does |
|---|---|
| `local-ci plan` | Show the workflows, units and setup steps that would run. Runs nothing. |
| `local-ci run` | Run them and report. Exit code 0 = success (or `no_work`: the changes trigger no workflow), 1 = failure, 2 = error (could not run, or the result lacks something the plan expects), 3 = `incomplete` (what ran passed, but steps or workflows that local-ci cannot reproduce were left out, so it is not a pass; the commit status is `error`), 130 = interrupted with Ctrl-C (a run stopped with `SIGTERM` or `SIGHUP` cleans up and ends by that signal). |
| `local-ci status` | Record the last run's result as the `local-ci` commit status (for a commit you have pushed since). |
| `local-ci cache status` | Show the disk space each part of the cache directory takes, the entries of the build cache, and how many worktrees are in use, kept, left by runs that are gone, or left by an older version. |
| `local-ci cache prune` | Remove the worktrees left by runs that are gone and empty the trash, before returning. `--kept` also removes the ones kept with `run --keep`, `--legacy` the ones left by a version that recorded no owner, `--build` empties the build cache. Run in a repository, it also drops the registrations there of local-ci worktrees whose files are gone. |

Common options (`plan` and `run`):

| Option | |
|---|---|
| `--base <ref>` | Compare against this ref. Default: origin's default branch. |
| `--sha <rev>` | Test this commit. Default: `HEAD`. Uncommitted changes are never tested. |
| `--all` | All checks: ignore `paths` / `paths-ignore`, and run workflows that are disabled on GitHub. `branches` and `types` still apply, so a workflow that only runs on pushes to `main` stays out. |
| `--include-disabled` | Also run workflows that are disabled on GitHub, with `paths` / `paths-ignore` still applied. `--all` implies it. |
| `--workflows-dir <dir>` | Also read the workflows in this directory (relative to the repository root, repeatable), besides `.github/workflows` or the directories `.local-ci.toml` names. GitHub never runs them, so this is the place for checks you only want before pushing. They are picked by the same triggers and `paths`, read from the commit like the rest, and named by their path in the output (`ci/local/mutation.yml › ...`). |
| `-w, --workflow <file or name>` | Only these workflows. |
| `-j, --job <id>` | Only these jobs, plus the jobs they `need`. |
| `-C <dir>` | Repository directory. |

`run` also takes `--parallel <n>`, `--containers <n>`, `--no-traffic-controller`, `--no-portool`, `--no-status`, `--keep`
(keep the worktrees), `--tail <lines>`, `--job-timeout <minutes>` (for jobs without `timeout-minutes`; waiting for
task-traffic-controller is not counted), `--all-os-legs` (below) and `--json`.

`--parallel <n>` is the number of units that run at once. Without it, every
unit (up to 16) starts when task-traffic-controller is installed, as each
step then waits there for CPU; without task-traffic-controller it is 4. It is
also the number of jobs the CPU is divided between (see
[the CPU share of a job](#the-cpu-share-of-a-job)).

## `.local-ci.toml`

An optional file at the repository root. Like the workflows, it is read from
the commit under test, so commit it before it takes effect.

```toml
# Read workflows from here instead of .github/workflows
workflows-dirs = [".ci/workflows"]
# Always run these, even when a recent result covers them
no-cache = ["e2e.yml", "api-ci.yml:contract"]

[setup]
# Env names that do not change what an install does (see below)
ignore-env = ["DATABASE_URL", "CI"]
```

| Key | |
|---|---|
| `workflows-dirs` | Directories to read workflows from, relative to the repository root. They replace `.github/workflows`; `--workflows-dir` still adds to them. Each one must hold a `.yml` / `.yaml` file at the commit. The workflows in the first one are named by their file name in the output (`ci.yml › test`), the others by their path. |
| `no-cache` | Workflows (`ci.yml`) or jobs (`ci.yml:e2e`) whose results are never reused (see *Reused results*). |
| `link-node-modules` | `true` to give units' copies links to setup's packages instead of a clone of each `node_modules`. Off by default. |
| `[setup]` `ignore-env` | `env:` names left out of the keys of setup steps. Off (empty) by default. |

**`[setup] ignore-env`.** An install that two jobs run in one directory with a
different `env:` (workflow, job or step) is not shared: each job runs its own,
so that one never sees the result of the other's. That includes variables the
install does not read, such as a `DATABASE_URL` that only the tests use. List
those names and jobs that differ only in them share one install, which runs
with the value of the first job that has it. A name that does change the
install (`NODE_ENV`) must not be listed.

**`link-node-modules`.** Even a copy-on-write clone makes, and later deletes,
one file per file, and dependencies are most of the files: in a monorepo of
twenty units that is millions of files per run, each deletion also checked by
fseventsd. With the key on, a copy's `node_modules` is a directory of links
to the packages setup installed. A step that may write into one (an install,
`npm rebuild`, `prisma generate`, `yarn`, `pnpm`, `patch-package`, or an npm
command from inside a script) first gets a copy of its own; so does a project
whose `node_modules` links out of itself (workspaces, `file:` dependencies)
or whose `tsconfig` emits declarations. It is off by default because a tool
that follows a link to where the package really is, outside the unit's copy,
may refuse it: TypeScript emitting declarations fails with TS2742 (handled
as said), and a Next.js build with Turbopack does not find `next` outside
its workspace root (not handled: do not turn the key on for a repository
that builds with Turbopack). Run everything once with `--no-cache` after
turning it on.

Without the file, local-ci reads `.github/workflows` as before. An absolute
path, a path outside the repository (`..`), an empty list and an unknown key
are errors.

**CI with local-ci only.** To keep GitHub Actions from running your checks and
run them with local-ci alone, move the workflows out of `.github/workflows`
(the only place GitHub looks) and point local-ci at their new place:

```
mkdir -p .ci && git mv .github/workflows .ci/workflows
printf 'workflows-dirs = [".ci/workflows"]\n' > .local-ci.toml
git add .local-ci.toml && git commit -m "Run CI with local-ci"
local-ci run
```

The triggers, `branches` and `paths` of the moved files apply as before, and
`local-ci status` still records the result as the `local-ci` commit status.
Workflows outside `.github/workflows` are never seen as disabled on GitHub.

## How a run works

0. **Reused results.** A unit whose jobs all passed (or were skipped) in the
   last 24 hours is not run again when nothing it depends on has changed: the
   files its workflow's `paths` cover (all files but its `paths-ignore`, or
   every file when it has neither), every file at the repository root
   (lockfiles, shared configuration), everything under `.github/` and the
   workflow directories, the merge base and the branch, the versions of Node
   and of the tools its steps name (npm, yarn, pnpm, dart, flutter, rustc,
   python3, go), and local-ci itself. This trusts `paths` to name what the
   workflow depends on, as GitHub does when a pull request changes nothing it
   covers: a change in a shared directory the workflow does not list does
   not run it again. The report shows such jobs as
   `PASS … cached (abc1234, 2h ago)`, still lists what they did not cover,
   and when every unit is reused nothing is checked out or installed. Units
   running `npm audit` or other advisory checks (also from a `package.json`
   script) are always run, since their result changes without the code; so
   are those listed in `no-cache`. `--no-cache` runs everything (and keeps
   the new results).
1. **Worktree.** The commit is checked out with `git worktree add --detach`
   under the cache directory. Your working tree is never touched, so steps such
   as `git add -A`, `cp .env.example .env` or generators that rewrite tracked
   files are harmless. The worktree is removed afterwards.
2. **Setup, once.** In CI every job runs `npm ci` on a fresh VM. Locally that
   would install the same dependencies once per job. Install steps
   (`npm ci`, `flutter pub get`, `npx prisma generate`, ...)
   and steps that several jobs repeat word for word at their start are run
   once, before any job, and skipped in the jobs. A step whose result cannot be
   carried to another job stays in its job: one that holds an expression,
   an `if:` or `needs`/`matrix`-dependent env, one that needs the job's
   services, one that writes `GITHUB_ENV`/`GITHUB_PATH`/step outputs, and
   Python venv / `pip install` steps (the venv lives at an absolute path).
   `local-ci plan` lists them with the reason. Installs that happen later
   anyway (a matrix job, a script that calls `npm ci`) go through an `npm`
   wrapper on `PATH`: a directory is installed once per run and lockfile, and
   later `npm ci` / `npm install` there are no-ops.
3. **Units in parallel.** Jobs linked by `needs:` (or writing the same build
   output) form a unit and run in order. Units run in parallel, each in its
   own copy of the worktree as it is after setup, as each CI job has its own
   checkout: a job that edits files for a while (a negative control, a
   formatter, mutation testing) is not seen by the jobs beside it. The copy is
   a copy-on-write clone (`clonefile(2)` on APFS, `cp --reflink=auto` elsewhere).
   With `link-node-modules = true` in `.local-ci.toml`, each `node_modules`
   holds links to the packages in setup's instead (see below). With task-traffic-controller,
   the entries of a matrix run at once too (up to `strategy.max-parallel`),
   each in its own copy. Jobs that start services or containers, run
   Playwright or Cypress, or look like integration / e2e tests, are
   *exclusive*. Their fixed ports and Compose projects are rewritten (see
   below). With portool and a `.portool.toml`, exclusive jobs without
   `services:` overlap freely, except a job that pins something rewriting
   cannot reach (a Compose file with `container_name:` or a literal host port,
   `docker run --name`): it takes a per-repository lock. Units with a
   `services:` job, and every exclusive unit without portool, hold that lock
   and never overlap, in this run or in another `local-ci` run of the same
   repository: a port their code picks for itself (a test server's) is not
   theirs alone. Either way, at most
   `--containers` exclusive jobs (default 2) run at once on the machine, across
   all `local-ci` runs, so databases do not pile up while their tests wait
   for CPU. Jobs that run at once each get a share of the CPU (see
   [the CPU share of a job](#the-cpu-share-of-a-job)).
4. **Report.** One line per job, the tail of each failing step, where the logs
   are, and the overall state: `success`, `failure`, `error`, `incomplete` (parts that cannot be reproduced here were skipped, or a workflow could not be run at all; `--json` lists them under `completeness.incomplete` and the exit code is 3) or `no_work` (nothing is triggered). `success` is only for a run whose result has everything the plan expects. A job that
   waited a second or more for something says what it waited for and how long
   it really ran: `PASS  ci.yml › e2e  3m00s (ran 2m20s · waited slot 5m00s, queue 40s)`.
   The job's `time` in `result.json` and in `--json` has the same split, in
   seconds: `work_secs` (the job running: its services' start and its steps), `wait_lock_secs` (the
   per-repository lock), `wait_slot_secs` (one of the `--containers` slots),
   `wait_queue_secs` (task-traffic-controller letting a step start, summed over the steps) and
   `wait_parallel_secs` (one of the `--parallel` places). It also has absolute wall-clock
   times in UNIX epoch milliseconds, to line a job up against other runs and the
   task-traffic-controller ledger: `requested_at_ms` (the run began to handle the
   job, before any lock, slot or parallel wait; for the jobs of a unit, when the
   unit began to wait), `started_at_ms` (the job's own execution began) and
   `finished_at_ms` (it ended), so that `requested_at_ms <= started_at_ms <=
   finished_at_ms` and `finished_at_ms - started_at_ms` is about `work_secs +
   wait_queue_secs`. They are 0 in a result written by an older version. The job's `secs`
   still counts from its first step and so includes `wait_queue_secs`; the
   other waits come before it. The waits of a unit before its jobs start go to
   its first job. Jobs that did not run, and reused results, have no `time`. Steps of a shell
other than bash or sh that print nothing count as waiting for task-traffic-controller until
they end.
5. **Commit status.** The state is posted as the `local-ci` commit status with
   `gh` when the commit is on GitHub. Before you push it is not; run
   `local-ci status` after pushing. `--no-status` skips this.

### The CPU share of a job

A build or a test runner uses every core unless it is told otherwise. Nine
jobs at once would each start as many threads as the machine has cores, and
tests time out that pass on CI, where each job has a runner of its own. So
the jobs of a run divide the CPU between them:

- **What is divided** (the budget): task-traffic-controller's budget (the
  `budget` of its configuration, by default the logical cores) when steps
  are wrapped in it, else the logical cores of the machine.
- **Between how many jobs**: `--parallel <n>` when it is given. Otherwise,
  with task-traffic-controller, as many as get 4 threads each (`budget / 4`,
  at least one): three jobs with four threads each rather than twelve with
  one. Without task-traffic-controller, the 4 units that run at once. Never
  more than the jobs the run can have running at once (its units, and with
  task-traffic-controller the entries of their matrices).
- **The share** is the budget divided by that number, rounded down, at least 1.

Each `run:` step of a job is then given the share in two ways:

- In its environment, as `CARGO_BUILD_JOBS`, `NEXTEST_TEST_THREADS` and
  `RUST_TEST_THREADS`. A variable that is already set is left as it is: in
  the environment `local-ci` is started with, in the workflow's, the job's
  or the step's `env:`, or by an earlier step through `$GITHUB_ENV`. A tool
  that reads none of the three is not limited: pass it the share yourself
  (`--jobs "$CARGO_BUILD_JOBS"`), or set its own variable in the job's `env:`.
  These variables win over a repository's own `build.jobs` in
  `.cargo/config.toml` and `test-threads` in `.config/nextest.toml`; set the
  variable in the job's `env:` to keep a number of your own.
- As the weight it asks task-traffic-controller for (`exec --weight
  <share>`), so the steps that run at once are the ones whose shares fit the
  budget, and the rest wait there. (task-traffic-controller lets a step of a
  nested run start after a long wait although the budget is full, 10 minutes
  by default, so that nothing waits for ever: with jobs longer than that the
  shares can add up to more than the budget.) The checks themselves are not
  changed: nothing is skipped and no timeout is shortened.

A job that has the whole budget is not limited: no variable is set and
task-traffic-controller works the weight out from earlier runs, as before.
That is a run whose jobs never run together: a single unit without a matrix
(or with a matrix under `strategy.max-parallel: 1`, or of an exclusive job
that holds the repository lock, whose entries run one after another), or a
budget under 8 with task-traffic-controller. `--parallel 1` limits nothing
by itself: the entries of a matrix still run together, and they divide the
budget between them. Setup steps run before the jobs, alone, and are not
limited either.

An empty variable counts as not set: `CARGO_BUILD_JOBS=` in the environment
`local-ci` is started with is replaced by the share.

When task-traffic-controller gates the steps but its budget cannot be read
(`task-traffic-controller inspect` fails, as in an older version, or has no
budget), the jobs are not limited: no variable is set, no weight is asked
for, and `local-ci` prints a warning once. A weight above a budget that is
not known could never be admitted.

The share is worked out by `local-ci run` only. A library caller of
`execute_plan` runs the jobs unlimited, unless it sets `Runner::job_threads`
itself; the `cpu` of a run result does cross the wire (`WireRunResult`) when
there is one.

When jobs are limited, the summary says how
(`cpu    budget 12 · 3 jobs at once · 4 threads each`), and `result.json` and
`--json` have it as `cpu`: `budget`, `jobs_at_once` and `threads_per_job`.

### The build cache for Rust jobs

Every job runs in a worktree of its own, so every job would build all its
dependencies again. For a job whose checkout has a `Cargo.toml` and a
`Cargo.lock` at its root, local-ci keeps the `target/` directory that the job
left when it passed, and gives a copy of it to the next job with the same key,
before its first step. Cargo then finds the dependencies Fresh. (The crates of
the workspace itself are built again: their sources are newer than the build.)
`actions/cache` and `Swatinem/rust-cache` stay no-ops; this does their job.

- **Key.** `rustc -vV` and `cargo -V` (asked in the job's worktree, so
  `rust-toolchain.toml` counts), the OS and architecture, the `Cargo.lock` of
  the checkout's root (the cache is of the root crate only), the content of
  `.cargo/config.toml` / `.cargo/config` and `rust-toolchain.toml` /
  `rust-toolchain` of the checkout, the workflow file and job id, the values of
  the job's matrix entry, and the variables that change a build as the job
  evaluates them (`${{ }}` is resolved, in the workflow's, the job's and the
  steps' `env`, and in the environment local-ci runs in): `RUSTFLAGS`,
  `RUSTDOCFLAGS`, `RUSTC_WRAPPER`, `RUSTUP_TOOLCHAIN`, `CARGO_*` (not
  `CARGO_BUILD_JOBS`, which is the job's CPU share, nor `CARGO_TERM_*`), and the
  coverage variables (`LLVM_PROFILE_FILE`, `CARGO_LLVM_COV_*`). The path of the
  worktree in a value is left out. A job that sets `CARGO_TARGET_DIR` or
  `CARGO_BUILD_TARGET_DIR` does not use the cache.
- **Toolchain.** The two version commands run once per job, before its steps,
  with `RUSTUP_AUTO_INSTALL=0` (a toolchain that is not installed is not
  installed for the key) and together within 20 seconds
  (`LOCAL_CI_BUILD_CACHE_PROBE_TIMEOUT_SECS`). If they fail or are too slow the
  job goes without the cache and a note says so; it never fails or waits
  because of it. A toolchain that a setup step of the job installs is not seen
  by the key: the key holds the one that is there when the job starts.
- **Restore.** Only when `target/` does not exist yet. The copy is a clone
  where the file system has them (`cp -c` on an APFS volume, `cp --reflink=always`
  on btrfs, XFS): a 1.3 GB `target/` takes about a second. Where the clone is
  not certain (on macOS: not one APFS volume), an entry up to 512 MiB is copied
  (`LOCAL_CI_BUILD_CACHE_COPY_LIMIT_MB`); a larger one is skipped and the job
  builds as before.
- **Save.** Only when the job passed and the key has no entry yet. An entry
  keeps what cargo owns: for each profile directory (`debug/`, `release/`,
  `<triple>/<profile>/`) its `deps`, `build`, `.fingerprint` and `.cargo-lock`,
  and the two files at the root of `target/`. Everything else is left out
  (`tmp`, nextest, `llvm-cov-target`, profile data, `criterion`,
  `incremental`, the output of examples). The entry is written under a
  temporary name while an exclusive lock is held on a lock file beside it, and
  renamed, so two jobs saving one key cannot mix them; the job's own `target/`
  is not touched.
- **Limits.** Entries are in `<cache dir>/build/<key>/`. Once the cache (the
  entries being written count) is over 20 GB (`LOCAL_CI_BUILD_CACHE_MAX_GB`),
  the least recently used entries are removed after a save; when that is not
  enough the new entry goes too, with a note. `LOCAL_CI_BUILD_CACHE_MAX_GB=0`
  turns the cache off. A temporary entry is removed only when its lock can be
  taken, that is, when its writer is gone. Entries are never deleted in place:
  they are moved to the trash, and one that cannot be moved stays, with a
  warning. `local-ci cache status` lists the entries, `local-ci cache prune --build`
  removes them all (and nothing else in the directory).
- **Off.** `local-ci run --no-build-cache`, or `LOCAL_CI_NO_BUILD_CACHE=1`.
- **Failures.** A restore or save that fails never fails the job: a note on its
  last step says why. The job's own steps are not changed.
- **Seen.** The summary says `build cache  restored 2 · saved 1`; the last step
  of the job has a note; `result.json` has `build_cache` (`key`, `restored`,
  `saved`) on the job, left out for jobs the cache does not apply to.

Only `local-ci run` uses it; `execute_plan` and the pinned executor do not (set
`Runner::build_cache` to turn it on in a library caller).

## What is supported

- Triggers `pull_request` / `pull_request_target` (with `types`, `branches`,
  `paths`, `paths-ignore`) and `push`. A run pretends to be a non-draft pull
  request into the base branch. `workflow_dispatch`, `schedule` and the rest
  are reported as not triggered, and so are workflows disabled on GitHub (read
  with `gh` when it is available) unless `--include-disabled` or `--all` is given.
- `run:` steps with `bash` (GitHub's `bash --noprofile --norc -eo pipefail`),
  `sh`, `python` or a custom `shell: ... {0}`; `working-directory`, `env`,
  `if:`, `continue-on-error`, `timeout-minutes`.
- Expressions: contexts `github`, `env`, `matrix`, `steps`, `needs`, `job`,
  `runner`, `strategy`, `inputs`, `vars`; operators; `success()`, `failure()`,
  `always()`, `cancelled()`, `contains`, `startsWith`, `endsWith`, `format`,
  `join`, `toJSON`, `fromJSON`, `hashFiles` (always empty). Anything else is an
  error, never a silent `false`.
- `strategy.matrix` with `include` / `exclude`, matrices computed from
  `needs.*.outputs`, `fail-fast`.
- `$GITHUB_OUTPUT`, `$GITHUB_ENV`, `$GITHUB_PATH`, `$GITHUB_STEP_SUMMARY`; job
  `outputs`.
- `services:` with Docker. Each container gets a free host port, and
  `localhost:<port>` / `127.0.0.1:<port>` in the job's `env` and `run:`
  scripts (and `*PORT*` variables holding the port) are rewritten to it, so a
  database you already run on 5432 is not in the way. A host port the workflow
  never names that way is left as declared, since then the code itself
  defaults to it. `--health-cmd` is waited for.
- Fixed ports and Compose projects in exclusive jobs (below). A port the job
  sets itself — a number in a `*_PORT` variable of its `env`, or
  `TEST_DB_PORT=15432 docker compose ...` in a script — is given a free port,
  and every place the job uses it is rewritten the same way. `docker compose -p
  <name>` and `COMPOSE_PROJECT_NAME=<name>` get a suffix of the run, so two runs
  never share a project and the clean-up never stops one you started yourself.
- `uses:` — `actions/upload-artifact@v4` and `actions/download-artifact@v4`
  run for real within one run (next section); `actions/checkout` and caches
  are skipped; `setup-node`,
  `setup-python`, `setup-go`, `setup-java`, `setup-ruby`, `flutter-action`,
  `setup-dart` and `rust-toolchain` check that the tool is installed and warn
  when its version differs. Other actions are skipped and listed under
  `not covered`.

### Jobs for another OS (`runs-on`)

Every job runs on the host, whatever its `runs-on` says. local-ci maps each
label to an OS family: `ubuntu-*` and `linux` are Linux, `macos-*` is macOS,
`windows-*` is Windows. A self-hosted runner, or a label it does not know, is
run as before.

- **A matrix over the OS.** The legs of one matrix job that differ only in the
  value of an OS label (`os: [ubuntu-latest, macos-latest]` with
  `runs-on: ${{ matrix.os }}`) run the same steps on the same machine. When
  one of them is of the host's OS family, only that leg runs. The others are
  not run and are listed under `not covered`: `runs-on ubuntu-latest: Linux is
  not reproduced on this macOS host; the same steps ran as test (macos-latest)`.
  They count neither as a pass nor as a failure, so the run is `incomplete`
  (exit code 3, commit status `error`) when everything that ran passed: a
  repository with an OS matrix cannot be a full pass on one machine, and the
  run says so. Legs are twins only when their
  other matrix values are equal and nothing in the job (steps, `env`, `if:`,
  `outputs`, services; names are left out) reads the differing value, as
  `${{ matrix.os }}` or `matrix.os == '...'` do: a job that reads it has other
  steps on each OS and every leg runs.
- **A leg with no twin** (a lone `runs-on: ubuntu-latest` job, a matrix whose
  legs have other values) still runs on the host. Its line says so, `PASS
  ci.yml › build  [ran on macOS, not Linux (runs-on ubuntu-latest)]`, and
  `result.json` has the same text as the optional `os_note` of the job. This is
  a pass on the host's OS only.
- **`needs` and outputs.** A matrix job is one job for `needs`, as on GitHub:
  its `needs.<id>.result` is `success` when the legs that ran passed (a leg that
  was left out counts as skipped, not as a failure), and its `outputs` are those
  of the legs that ran. As the left-out legs run the same steps and produce the
  same outputs, a job that needs the matrix sees what the leg of the host's OS
  made. The left-out leg is still listed under `not covered`.
- **Run every leg.** `--all-os-legs`, or `LOCAL_CI_ALL_OS_LEGS=1`, runs all the
  legs on the host as before (each with the note above). The legs that are not
  run are not counted when the CPU is divided between jobs that run at once. A result of a run
  without it is not reused by a run with it.

### Artifacts (`upload-artifact@v4`, `download-artifact@v4`)

Within one workflow run, an upload copies its files into the area of its
workflow (`<run dir>/artifacts/<workflow>/<name>/v<N>/`, `<workflow>` being the
workflow file's path made safe as a directory name), read-only, and writes a
receipt (name, each file's relative path, size and SHA-256, the plan digest,
the attempt and the job) before the step completes. A download reads the
receipt, checks every file against it and only then places the files, so a job
that `needs` the uploading job gets exactly what was uploaded. As on GitHub,
artifacts belong to a workflow run: two workflows may each upload a `coverage`
without a conflict, and a job cannot download what a job of another workflow
uploaded (it fails as not found, `pattern` included). A run is not a `success` while an
artifact its plan requires (a literal name, a step that always runs — no `if:`,
or one that is only `always()`, `success()`, `!cancelled()` or `true` — and
`if-no-files-found: error`) has no receipt.

A step local-ci cannot run (the table below) does not stop planning: the jobs
of that workflow that the run would start (jobs a `--job` filter leaves out are
not looked at; jobs whose `if:` is decided when they run are) are not run and
are reported as an `error`, in the output and in `--json`, naming the option,
the workflow, the job and the step. The other workflows run as usual.

Units that use these actions are kept in the results cache. A kept result
carries the artifacts its unit uploaded (receipt and files); reusing it puts
them into the area of its workflow in the run, every file checked against its receipt first
(the receipt then carries this run's plan digest and attempt, and
`restored_from` says which attempt made it), and a file that does not match
makes it a miss: the unit runs again. A unit that downloads is reused only when
each artifact it took (named literally) is, in this run and workflow, the same artifact
(same files, sizes and SHA-256) the result was kept against, which is the case
when the unit that uploads it is reused too. A download by `pattern`, or by a
name that is an expression, is never taken from the cache.

| | Supported | Not supported (the workflow's jobs fail before they run) |
|---|---|---|
| Action | `actions/upload-artifact@v4`, `actions/download-artifact@v4`; a 40-character commit SHA is run as v4 (its version cannot be checked, so the run warns once) | other major versions (`@v3`, `@v5`), sub-actions such as `upload-artifact/merge` |
| `name` | upload: a name without `/ \ : " < > \| * ?`, default `artifact`; download: one artifact by name | |
| `path` (upload) | several lines, `*` `?` `**` globs, `!` exclusions, a directory takes what is under it; stored relative to the common ancestor of the lines; hidden files only with `include-hidden-files: true` | an absolute path, `..`, a backslash |
| Multiple files | yes (sorted, each with its own size and SHA-256) | symlinks are not followed or uploaded |
| `if-no-files-found` | `warn` (default: the step passes with a warning), `error` (the step fails), `ignore` | other values |
| `overwrite` | a second upload of a name is a conflict by default; `overwrite: true` records a new version, and a download takes the newest | |
| Matrix | the name must read every key of the matrix (`name: report-${{ matrix.os }}-${{ matrix.node }}`; keys that `include` only adds to a variant are not asked for) or `${{ strategy.job-index }}`; with or without `overwrite` | a name that leaves a key out, so two variants would upload one name; one that holds the whole matrix (`${{ matrix }}`, `toJSON(matrix)`), which is JSON and always fails |
| `path` (download) | a directory under the workspace; the default is the workspace itself | an absolute path, `..`, a path through a symlink |
| `pattern`, `merge-multiple` | yes: without `merge-multiple` each artifact goes to `<path>/<name>/`; two artifacts writing one file is an error | |
| `retention-days` | 1 to 90, recorded in the receipt (local runs are kept until pruned) | |
| `compression-level` | 0 to 9: accepted, no effect (files are stored as they are); recorded in the receipt | other values |
| Across runs, GitHub's side | | `run-id`, `repository`, `github-token`, `artifact-ids`, `skip-decompress` and any other option |

A download does not wait for the uploading job to finish: it needs the receipt
only, so it must run after the job that uploads (`needs:`); a download with no
receipt is an error that says so. Moving artifacts to and from another host is
not part of local-ci.

A step whose script holds `github.token` is run with bash, sh or python (the
default shell is bash); the script is handed over a pipe and never written to a
file. With another shell (`shell: pwsh {0}`, `shellcheck {0} && bash {0}`) such
a step is not run and the run is an `error`. The value is replaced by `***` in
logs, labels and notes; a changed form of it (base64, say) is not hidden.

Not covered: steps that read a secret other than `github.token` (which gets
this machine's `gh auth token`, never written to a file of the run), steps that push,
publish or deploy (`git push`, `gh release`, `npm publish`, `aws`, `kubectl`,
`terraform apply`, ...), steps that install system packages (`sudo`,
`apt-get`, `brew install`, ...), `container:` jobs and reusable workflows. A
`git push` from inside a script is pointed at a URL that does not exist. `pip install` goes into a virtualenv of the run and `npm install -g`
into a prefix of the run, so neither changes your machine.

## Working with other tools

- **[task-traffic-controller](https://github.com/t09tanaka/task-traffic-controller)**: when it is installed,
  every `run:` step, setup included, is wrapped in `task-traffic-controller exec`, so heavy steps
  wait for CPU instead of piling up. Timeouts start when the step gets its turn
  (a bash or sh step marks it as its first act; for other shells, at the first
  output in the log that is not task-traffic-controller's),
  and the wait does not count against the job's `timeout-minutes` (or
  `--job-timeout`) either: a job is over its time when its setup and the
  running of its steps add up to more than that. A step that is stopped at the
  job's limit, not its own, says so (`timed out after 4.0s (what was left of the
  job's 0.1 min)`), and a step that would run after the job's time is used up
  (an `if: always()` step) is not started (`not started: the job timed out (0.1 min)`).
  The steps of jobs that run at once ask for the job's
  [CPU share](#the-cpu-share-of-a-job) as their weight. Running `local-ci`
  itself under it needs task-traffic-controller 0.9 or later, where a nested
  `exec` waits for its turn too (before 0.9 it started at once, and every job
  ran on every core). Give the outer command a small weight, as `local-ci`
  only starts the steps and waits for them:
  `task-traffic-controller exec --weight 1 -- local-ci run`. The steps then
  take from the budget what they ask for, and no more is held for `local-ci`
  itself.
- **[portool](https://github.com/t09tanaka/portool)**: in a repository with a
  `.portool.toml`, each exclusive job without `services:` asks portool for a
  port block of its own
  worktree copy and gets `.env.portool` (`TEST_PORT=...`) in its environment, so
  a script that falls back to a fixed port (`TEST_PORT ?? 4000`) uses one no
  other worktree has. The workflow's own `env` still wins. The block is given
  back when the copy is removed. `--no-portool` turns this off.

## Splitting a test job into shards (library)

`local_ci::shard` divides the tests of one job among shards that run side by
side; nothing is split unless the caller asks (`FrozenPlan::with_shards`), and
the CLI never does. A runner adapter lists the tests, names each shard's tests in
a command and reads what a shard reported: `cargo test`, Vitest and Jest (by
file or test name), Playwright (by file, or by test where the suite is fully parallel)
and Cypress (by spec file). The shards must cover the listed tests exactly once, each shard may report
only what it was given, a report that cannot be read or holds no test is a
problem, and a job that cannot be split safely stays one job with the reasons
(`NotSplit`).

A job is not split when:

- its own command has a name filter, tag, skip or an argument that is not known to
  leave the tests alone, when a test path is part of another, or when its step is
  not one plain command in the directory the tests were listed in (`continue-on-error`
  included);
- it collects coverage (`--coverage`, `--cov`, `nyc`, `COVERAGE=1`, a coverage setting in the Vitest
  or Jest configuration or `package.json`): `exec` does not collect the shards' reports.
  `shard::coverage::merge` is a library function that nothing here calls; the caller that
  collects the reports wires it. It puts together Istanbul `coverage-final.json` and LCOV
  reports, only for the same code in every shard (Vitest's `v8` provider maps each shard's
  files differently, so it stops at `Mismatch`); `cargo llvm-cov` profile data and other formats
  are an `Unsupported` error;
- it starts something the shards would share: job `services`, a container tool
  (`docker`, `podman`, `kubectl`), a server or a port (`npm start`, `pnpm dev`, `next dev`,
  `localhost:3000`, a `PORT` variable, a command sent to the background), a database tool
  (`psql`, `prisma migrate`) or a service action. That is a list of words over what the workflow
  says. The main defence is the type of the end-to-end adapters: they take an `Isolation`, which
  is `None` until the caller declares (`Isolation::Declared { evidence }`) that each shard has its
  own database, port and browser profile;
- (Playwright) the configuration has a `webServer`, a global setup or teardown, `repeatEach` or a
  `shard`; a project depends on another or has a teardown (the listing script compares `--list
  --project P` with and without `--no-deps`, and reads the configuration file by text); or the
  runner is older than 1.56 (no `--test-list`). Unless the suite is fully parallel, the tests of
  a file are one unit, and so are those of a file that declares serial tests; read a shard's
  report with `collect_given` so that each unit is reported once;
- (Cypress) the configuration says which files are specs (`specPattern` and friends), or the
  command has `--config-file` or `--project`: the adapter lists `cypress/e2e/**/*.cy.{js,jsx,ts,tsx}`
  and does not evaluate the configuration. A shard runs Cypress through its module API in a small
  script, which prints one JSON line with a result per spec file and exits non-zero when a test failed.

## Planning an already captured source (library)

`build_frozen_plan_from_source(store, manifest, options)` validates the
manifest and objects, materializes a private temporary view without secrets,
and applies the existing configuration, workflow parser and planner. It does
not need an `ExecContext`, a Git checkout, a command runner, the network, or
execution resources. The view is destroyed before the call returns.

`PinnedPlanOptions` supplies `Filters`, an explicit disabled-workflow list,
and optional frozen `base_branch` and `changed_files`. `None` means unknown;
`Some(vec![])` means a known empty change set. `workflow_dirs_override` is an
optional authoritative list from the caller's resolved configuration: `None`
uses the captured configuration/default, `Some(empty)` is invalid, and a
nonempty list replaces the captured workflow directories before extra
`filters.workflows_dirs` are appended. The resolved directories are included
in the frozen configuration and its digest. A selected, enabled workflow
whose branch or path filter requires unknown metadata returns a static typed
`PinnedPlanError`. `all` bypasses path filters, as in ordinary planning; branch
filters still apply. No branch or base is inferred from HEAD or a live tree.
Submodule captures are currently refused with `UnsupportedSource`.

Captured plans use schema 7. Their `source.pinned` records source kind,
manifest digest, repository identity, relative capture directory and event
metadata availability; `source_id` and the plan digest bind those values.
`SourceRef.execution_commit` and `base` are now `Option<String>`: directory
sources have neither, and a captured Git source can have an unknown base.
Ordinary Git plans retain schema 6 and their existing JSON/digests; their
string fields deserialize as `Some` and absent `pinned` as `None`.

The existing Git execution entry points reject every captured plan with typed
`UnsupportedExecution` before tools, commands, workspace writes or execution
resource registration. This API plans captured inputs; source-aware execution
is separate. `Runner::new`, `run_recipe`, `run_units`, `release_ports`, and
`report::assemble` now return `Result` so direct callers also receive this
typed refusal, including an empty recipe or report. A manifest digest
establishes consistency, not authenticity: the
caller is responsible for the provenance of manifests and object stores.

## Capturing a working directory (library)

`source::capture_source` fixes the input of a run as the working directory is
right now (staged and unstaged changes, untracked files, deletions, modes,
symlinks, submodules, LFS payloads) in a private store, and describes it in a
`SourceManifest`. The repository itself is only read.

- The first capture of a repository copies the history of `HEAD` into a
  private bare repository under the store, so a rewrite or `git gc` of the
  original cannot break an export later; the next captures fetch only what is
  new.
- The manifest records the settings that turned the captured bytes into Git's
  blobs (`core.autocrlf`, `core.eol`, `core.safecrlf`, the attribute rules that
  are not in the tree, and the `filter.<name>.*` definitions in use). When the
  repository a run happens in already reads the same values (a run in the
  original itself always does) nothing is written. A worker whose settings
  differ repeats them in its worktree only (`extensions.worktreeConfig` is the
  one switch written to its shared config, and only in a clone that was not
  captured from); if the original's own settings changed after the capture the
  run stops with `SourceError::OriginalConfigChanged` instead of writing to
  it. Either way `git status` agrees with the commit. One known limit: Git
  reads `info/attributes` before `.gitattributes`, but a worker repeats those
  rules where Git reads them last, so when they disagree about a path the
  result is `SourceError::WorkspaceNotClean`, never silently different
  content. A worker without the program of a filter the capture ran gets
  `SourceError::FilterMissing` and can be left out; a workspace that Git still
  sees as changed is `SourceError::WorkspaceNotClean`.
- `source::export_objects` / `export_from` bundle the capture down to the
  merge base with origin; `source::export_with_basis` cuts it at commits the
  receiving host says it has instead (an empty list gives the whole history,
  for a host with no origin; at most `source::MAX_HAVE` = 256 commits). Each
  distinct list leaves one bundle file in `bundles/` of the store (its name
  ends in 32 hex digits for the list); removing them is the caller's job,
  and a caller that takes the list from a network peer should bound how many
  distinct lists it accepts for one source.
  `source::import_objects` brings a bundle into a clone;
  `import_objects_for(ctx, clone_dir, bundle, m, nested)` is for a receiving
  host with no store: `nested` holds the manifests of the dirty submodules
  below `m` at every depth (at most `source::MAX_NESTED` = 256), exactly the
  ones that are reached from `m`, and each one's snapshot ref, tree and
  gitlinks are checked against its manifest. Both fetch the refs of the
  source without force, all or none (`git fetch --atomic`, Git 2.31 or
  later): the snapshot ref must be the execution commit of the manifest, a
  ref that is already in the clone with another value is refused and kept,
  and an import that is refused leaves no ref behind that it made (the
  objects stay until the next `git gc`). A manifest holds at most
  `source::MAX_DIRTY_SUBMODULES` = 1024 dirty submodule entries, nested ones
  counted in. The digest of a manifest has no key: it shows that the
  manifest holds together, not who made it, so where a manifest and a bundle
  come from is for the caller to establish.
- Not supported yet: shallow clones (`git fetch --unshallow` first) and
  repositories that use SHA-256 object names. Both are refused with an
  explicit error.

`source::capture_directory` does the same for a directory that is not a
repository.

### Secret files

`CaptureOptions::secret_files` (the `[source] secret_files` globs of
`.local-ci.toml`) names files a run needs that must not travel with the
source, a test `.env` for instance. They never enter the object store, the
manifest's entries or an exported bundle of objects: the manifest records
their paths and an HMAC of each under `secret_key` (16 bytes or more), and
their content goes to a secret bundle of its own (mode 0600), whose path
`SourceSnapshot::secrets` returns. `materialize_secrets` places them in a
workspace after checking each against its HMAC.

- In a repository, a secret file is an untracked file a declaration matches,
  ignored or not. A tracked file is commit content and stays that (with a
  warning). In a directory capture, any file a declaration matches is a
  secret file; `exclude` is applied first.
- Only a regular file inside the root is read. A symlink (the file, or a
  directory above it), a FIFO or a device at a secret path stops the capture
  with an error; it is neither followed nor skipped. A `required` declaration
  that matches nothing stops it too (`SourceError::RequiredSecretMissing`); an
  optional one is a warning.
- A secret file is at most 1 MiB and those of one capture together at most
  8 MiB; above either the capture stops with `SourceError::SecretTooLarge`
  (the file is not skipped).
- `secret_root`: take the secret files from another working tree than the one
  that is captured. This is for the capture of a commit, which runs in a clean
  checkout that has none of them: point it at the tree the commit came from.
  The files are chosen, read and re-checked there by the same rules, and a
  file there that the captured commit tracks is left out as commit content.
  A file that tree tracks and the captured commit does not is in neither
  place (not content, not a secret file); a warning names it.
  Only for `capture_source`.
- `secret_bundle`: write the bundle to this path instead of
  `<store>/secrets/<source_id>.json`, which two captures of the same source
  share. The file is created, never replaced, and its directory is not made:
  either case is `SourceError::SecretBundlePath`, and a capture that fails
  after writing it removes it. Nothing is written when no secret file was
  found. The directory has to be on a filesystem with hard links. A process
  killed during the write can leave a `.secrets.tmp-*` file (0600, the secret
  files in plain) next to the path: sweep those when you keep the directory.

## Driving a run from another host (library)

`local_ci::driver::Driver` advances one run over a frozen plan without doing
any work itself: it decides what is ready, how many tasks may be held at once,
when a matrix or a set of shards is complete, and what a cancel reaches. It
reads no clock, file or process, so a host that distributes tasks to workers
and a host that runs them in place share the same progression. Tasks are
executed with the existing Runner (`execute_task`, `execute_git_dispatch`,
`execute_pinned_task`, `execute_pinned_dispatch`), which evaluates jobs with
the same code the driver uses to expand a matrix.

```rust
use local_ci::driver::{ActionKind, Driver, DriverConfig, Input, DRIVER_VERSION};

let config = DriverConfig { schema_version: DRIVER_VERSION, max_parallel: 4 };
// `Driver::new(&plan, config)` when no matrix depends on another job's output
let mut driver = Driver::with_evaluation(&plan, config, &source, facts)?;

// every change is proposed, stored by the host, then applied
let record = driver.propose(Input::Advance)?;
journal.append(serde_json::to_string(&record)?)?;
driver.apply(&record)?;

for action in driver.pending_actions() {
    match &action.kind {
        ActionKind::Ready { task, inputs } => { /* a task of the plan */ }
        ActionKind::DerivedReady { dispatch, inputs } => { /* a member of a dynamic matrix */ }
        ActionKind::Cancel { task, token } => { /* tell that worker to stop, then Ack */ }
        ActionKind::Finished => { /* every task is decided */ }
    }
}
```

**Progress.** `propose(input)` computes a `Transition` and changes nothing;
`apply(&transition)` recomputes it and refuses a record that was altered, is
out of order, or belongs to another plan or configuration. Replaying the stored
records into a new `Driver` gives the same state (`Transition::from_json`), so
the journal is the only thing a host has to keep. `Input::Advance` makes the
first tasks ready; later inputs advance the run by themselves.

**Events.** `pending_actions()` is the outbox: `Ready`, `DerivedReady`,
`Cancel` and `Finished`, each with a stable `id`. A ready action leaves the
outbox when its task is claimed (acknowledging it changes nothing); a `Cancel`
when the host sends `Input::Ack { action }` or the task ends; `Finished` when
it is acknowledged. An action that is still there after a
restart has to be delivered again, so delivery must be idempotent on `id`.
The members of a matrix that is an expression over `needs` appear as
`DerivedReady` once the job that names them has settled: the driver derives
them from the verified source (`git_evaluation_source` or
`PinnedExecution::evaluation_source`) and the host's `PortableFacts`, and the
worker derives the same dispatch again before it runs anything.
`requirements(task)` gives the `runs-on` of a task for placement.

**Permits.** `Input::Claim { task, token }` admits one attempt and takes a
permit; it is refused when `max_parallel` or the `max-parallel` of the job's
matrix is used up (`admitted()` counts the claims held). The host commits
`Input::MarkDispatched` before it sends the work. The permit is released by
exactly one of:

- `Input::Settle { task, token, result_digest, result }`, the worker's
  `WireTaskResult`. The driver checks it against the task before it publishes
  outcomes, joins the matrix (`fail-fast` included) or the shards
  (`shard_join`), and makes the jobs that need them ready.
- `Input::AbortBeforeExecution { task, token, reason }`: the host could not
  start a claim that was never dispatched.
- `Input::Fence { task, token }`: the host has revoked a claim that will not
  answer (a lost worker). The job fails with no outputs, and a result that
  arrives later under that token is refused. The driver cannot know that the
  worker stopped; the host answers for what it still holds.

A release is never refused because the driver's state is full. When the
jobs it would make ready do not fit, the claim is released all the same and
the run stops with `infrastructure_failure()`, as below.

**Cancel.** `Input::Cancel` cancels the run: tasks nobody claimed are decided
as cancelled, and each claimed task gets a `Cancel` action. A claim keeps its
permit until its result, abort or fence arrives; an acknowledged `Cancel` is
not a stop. `fail-fast` uses the same requests for the other members of a
matrix or a set of shards.

**Results.** `outcome(workflow, job)` is the published outcome of a job (a
matrix joined as on GitHub), `core_decision(task)` says when the driver decided
a task without a worker (skipped, cancelled, host failure), and
`infrastructure_failure()` must be checked before a run is reported as passed:
a result too large for the driver's bounded state is kept as its digest and
statuses (`unavailable_worker_receipt`) and stops the run instead of
publishing outputs it does not have. The bounds and what a host has to do
about them are in the module documentation (`cargo doc`, `local_ci::driver`).

`tests/driver_host.rs` is a complete host in about two hundred lines: a
dynamic matrix run through the driver and the Runner, a replay of its journal,
and a lost worker that is fenced.

Not in this version: the `local-ci` command still schedules with its own
Runner loop, retrying a shard under a new attempt is the host's work (a fenced
or failed shard fails its join), and artifacts move through the host's own
storage (`artifact_io`), not through the driver.

## Files

The cache directory is `~/Library/Caches/local-ci` on macOS and
`$XDG_CACHE_HOME/local-ci` (or `~/.cache/local-ci`) elsewhere; set
`LOCAL_CI_HOME` to move it. `LOCAL_CI_HOME` must be an absolute path (an empty
or relative one is refused), and should be a directory of local-ci's own. It holds the worktrees while they run
(`wt/<sha>-<pid>`, one `wt/<sha>-u<n>-<pid>` per unit and one `wt/<sha>-c<n>-<pid>` per matrix entry run at once), the last run
of each worktree (`runs/<repo>/<time>-<sha>/` with one log per step and
`result.json`; the runs of a worktree that is gone are dropped), the results that may be reused (`results/<repo>/`, a day at
most), and the locks.

Nothing a job builds is kept from one run to the next: each run checks the
commit out afresh, and a worktree holds whatever the jobs built in it (a
`target` directory, `node_modules`), so a run removes its worktrees, its
copies and its tools directory (`wt/<sha>-<pid>.tools`) when it ends, whether
the jobs passed, failed, or the run was stopped with Ctrl-C, `SIGTERM` or
`SIGHUP`. The directories are moved to `trash/` and deleted in the
background. Only the names local-ci gives there are ever deleted from
`trash/`.

A run stopped with Ctrl-C exits with 130. A run stopped with `SIGTERM` or
`SIGHUP` cleans up first and then ends by that same signal, so a supervisor
or a shell that waits for it sees that the signal ended it. A signal that was
ignored when local-ci started stays ignored: `nohup local-ci run … &` goes on
when the terminal closes.

A run that is killed outright (`SIGKILL`, a crash, a power loss) cannot do
that. Each run holds a lock on `wt/.owner-<pid>` while it lives, and every run
starts by removing the entries whose owner is gone, by dropping their
registration from the repository they belong to (`git worktree prune`) and by
deleting what is still in `trash/`. An entry is removed only when its owner
is known to be gone: its owner file is there and no longer locked. Age is not
looked at, and the entries of a run that is alive, in another terminal too,
are left alone. `local-ci cache status` shows what the directory holds, and
`local-ci cache prune` does the same removal without a run and waits for the
deletion.

A run unregisters its worktrees in the repository each one is registered in,
not in the checkout the run was started from, so a linked worktree that is
removed while a run from it goes on does not leave registrations behind.
When registrations are left anyway (by an earlier version, or by a run killed
between deleting a worktree and unregistering it), nothing in the cache says
whose they were. Run `local-ci cache prune` in the repository: it also drops
the registrations there of worktrees under the cache directory whose files
are gone. Locked registrations are left alone. When it finds one of its own,
it runs `git worktree prune`, as a run does, so any other registration of
that repository whose directory is gone is dropped too.

Two kinds of entries are never removed by a run:

- Worktrees kept with `--keep`. Each gets a mark of its own
  (`wt/.kept-<entry>`) when the run ends, and stays until
  `local-ci cache prune --kept`. A run with `--keep` that is killed before it
  ends has written no mark yet: its worktrees are removed by the next run,
  like those of any killed run.
- Worktrees with no owner file, left by a version that recorded none
  (an earlier version). They may have been kept on purpose, so a run only says how
  many there are. `local-ci cache prune --legacy` removes those whose pid no
  process has.

The cache directory must not be shared between pid namespaces (containers
that mount the same directory, a network file system): a pid then no longer
names one process. A run that finds its owner file held by another live
process stops with an error; give each its own `LOCAL_CI_HOME`.

## Development

```sh
(cd examples/sharding/node && npm ci)   # the shard adapter tests run real Vitest and Jest
(cd examples/sharding/playwright && npm ci && npx playwright install chromium)   # and real Playwright
(cd examples/sharding/cypress && npm ci && npx cypress install)   # and real Cypress (its test is ignored off Linux)
cargo test --features browser-tests   # without the feature, the tests that start a browser are left out
./scripts/coverage-gate.sh   # 100% line coverage on the pure modules (no browser tests)
```

The pure decision modules (`src/matrix.rs`, `src/filter.rs`, `src/source/gate.rs`,
`src/dag.rs`, `src/frozen.rs`, `src/artifact.rs`, `src/shard.rs`, `src/shard/adapters.rs`,
`src/shard/coverage.rs`, `src/shard/isolation.rs`, `src/demand.rs`, `src/result_wire.rs`) are gated on 100% line coverage over the whole test suite (`src/wire.rs` and `src/digest.rs` are held to the mutation gate only: the line counts of their generic and per-crate code are not stable enough for a 100% line gate).
All of them but `src/dag.rs` are also gated on zero surviving mutants. The shared driver and the job evaluation it shares with the Runner (`src/driver.rs`, `src/driver/atomic.rs`, `src/driver/dispatch.rs`, `src/driver/shards.rs`, `src/job_evaluate.rs`) are held to both gates as well. The coverage gate needs `cargo-llvm-cov` and the `llvm-tools-preview`
component. If `cargo llvm-cov` cannot find the LLVM tools, point it at them:

```sh
export LLVM_COV=~/.rustup/toolchains/stable-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/bin/llvm-cov
export LLVM_PROFDATA=~/.rustup/toolchains/stable-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/bin/llvm-profdata
```

Mutation testing uses [cargo-schemata](https://crates.io/crates/cargo-schemata), which reruns the
tests once per mutant. Each module is checked against the test targets that pin it (the exact
commands are in `.github/workflows/ci.yml`):

```sh
cargo schemata --file src/matrix.rs --file src/filter.rs --file src/source/gate.rs -- --lib --test job_dag --test source_capture
cargo schemata --file src/frozen.rs -- --lib --test job_dag --test frozen_plan --test source_plan --test shard_plan --test wire_api
cargo schemata --file src/artifact.rs -- --lib artifact
cargo schemata --file src/shard.rs -- --lib shard::tests --test shard_plan
cargo schemata --file src/demand.rs -- --lib --test task_demand
cargo schemata --file src/wire.rs --file src/result_wire.rs --file src/digest.rs -E 'replace wire_version -> u32 with 1' -- --lib --test wire_api --test frozen_plan --test shard_plan --test job_dag
```

The wire gate leaves out the mutant `wire_version -> 1` (`-E`): the version is 1, so it changes nothing.

`cargo schemata` copies the sources into `target/schemata/` and points `TMPDIR` there, which is
inside the repository. Tests that need "not a git repository" therefore set
`GIT_CEILING_DIRECTORIES`.

`cargo schemata --file` exits 0 when the glob matches nothing, so CI first runs
`scripts/mutation-targets-check.sh <file>...` (built on `--list`) and fails if any target file
produces no mutants, for example after a rename.

## License

MIT

### Executing captured tasks (library)

`source_execute::PinnedExecution` binds a verified frozen plan and source
manifest once. Actual materialization verifies the captured file objects;
subsequent setup/task entry guards check the retained plan and root identity,
without recreating the source. `materialize_workspace` handles directory inputs.
`materialize_git_workspace` uses the existing Git worktree materialization policy
at the captured execution commit, from a repository holding the imported bundle.

A worker that already owns materialization can call `verify_workspace` before
restoring secrets or applying setup. This checks the secret-free payload,
recursively including captured submodule payloads, and derives Git HEAD/index
requirements from the manifest: worktree input keeps its original HEAD and
captured index; commit input keeps the captured commit. A plain directory never
receives synthetic Git identity. Submodule payload verification does not create
nested Git administrative directories.

For detached helpers, persist a verified capability with `write_receipt` in a
`PrivateReceiptDirectory`, opened below the trusted worker-state directory FD
and the accepted attempt directory. This state must be private and external to
the project. Recovery uses `restore_workspace` with the accepted attempt and
generation; it checks the SDK-issued receipt, source/plan binding, every workspace
path, and the root device/inode while allowing declared setup mutations. These
receipts share the worker UID trust boundary of accepted attempt records; they
are not credentials for accepting arbitrary serialized checkouts.

Each capability has independent scripts, matrix copies and container resource
names, with shared sequence allocation across its setup/task runners. Recovery
preserves setup tools and install markers while issuing a fresh execution
namespace. Hosts retain workspace, secret restoration and cleanup ownership via
`PinnedWorkspaceProvider`.


### Binding a workspace the host has added to (library)

`PinnedExecution::verify_workspace` binds a workspace only when it equals the
captured source byte for byte. A host that restores secret files, copies
dependency directories in or keeps the output of a prepared setup before it
binds declares those paths instead:

```rust
use local_ci::source::RelPath;
use local_ci::source_execute::WorkspaceOverlay;

let overlay = WorkspaceOverlay {
    paths: vec![RelPath::new("node_modules"), RelPath::new("packages/app/dist")],
    secret_files: vec![RelPath::new(".env")],
};
let workspace = input.verify_workspace_with_overlay(&ctx, workspace, &overlay)?;
audit(workspace.unverified_paths()); // sorted, what was not compared
```

A declared path and everything below it is not compared; every other file must
still match, an undeclared extra file is refused, and the Git HEAD and index
are checked as before. A path has to be relative and normalized (no empty, `.`,
`..` or `.git` component), must not be a symbolic link or lead through one or
through a file, and must not touch a submodule. A secret file has to be one the
manifest declares, and a regular file when it is there. The content of the
declared paths is the host's word: steps run with it unchecked. A workspace
restored from a worker receipt does not carry the list.

### Verified scheduling source (library, in development)

`source_execute::git_evaluation_source(ctx, repository, frozen_plan)` reads the
ordinary Git plan's exact full commit and returns `VerifiedEvaluationSource`.
It rereads the original `runs-on` facts from the source instead of trusting
the frozen workflow projection. Each workflow must be a regular blob at the exact literal path,
match the frozen projection, and pass raw-byte Git OID and SHA-256 checks.
Working-tree edits and replacement refs cannot retarget these reads.

Ordinary Git planning and this reader require Git with the `--no-lazy-fetch`
global option; unsupported versions fail explicitly. Missing local objects fail without fetching, applying
filters or prompting for credentials. It reads at most 4096 workflows and 4MiB
of their combined contents, with bounded stdout/stderr and cooperative 5-second
command / 30-second request deadlines. Owned command groups are killed before
their leaders are reaped on every return path. The Git executable and repository
object database remain the host's trust boundary: this does not scan or verify
the whole commit/tree object graph against hostile database replacement.

The planner shares that raw workflow-body reader and one combined byte/count
budget, preserving trailing YAML block-scalar newlines instead of trimming
workflow text. Ref metadata is resolved once to an immutable commit before
listing and reading. Listing uses the same owned Git process boundary with
literal NUL-terminated paths and strict UTF-8 decoding. A separate aggregate
metadata allowance covers all directories: at most 4096 listed entries (including
non-workflow files), at most 4096 bytes per path and about 16MiB of raw listing
bytes. Those limits do not reset per directory and do not consume the separate
4MiB workflow-body budget. Other legacy metadata calls remain outside these
bounds; no hard filesystem syscall deadline is claimed. Git plans previously frozen with trimmed
block-scalar text must be rebuilt from the same immutable commit before using
the strict scheduling verifier; no trimming compatibility fallback is applied.

Captured inputs still use `PinnedExecution::evaluation_source()` and a real
manifest binding. The unpublished scheduling record is schema 2 with explicit
`EvaluationBinding::Captured { manifest_digest }` or `Git { execution_commit }`;
Git inputs never receive a synthetic manifest digest. Decoded records are
untrusted: `verify_git_evaluation_source` or the captured verifier must recompute
before accepting them. The verified capability has no deserialization or
unchecked construction API. This is a scheduling-provenance boundary, not a
replacement of the ordinary CLI Runner; the shared host adapter remains work
in progress.

### Verified ordinary Git dynamic execution (library, in development)

`execute_git_dispatch` executes one member from a real `Driver::DerivedReady`
action. It rereads verified Git source and rederives the complete dispatch from
the frozen plan, original scheduling facts, host-authorized `PortableFacts` and
`TaskInputs` before constructing a Runner. Captured execution uses the same
shared dispatch validator. A decoded dispatch or a caller-made task is not an
execution capability. The host still authenticates the committed claim, inputs,
facts, worker selection and delivery; this API does not advance Driver state.

This is an attempt-scoped entry with the same prepared-checkout contract as
`execute_task`. The caller supplies its own context and workspace, tools and
run directory. Context-wide container cleanup and workspace port release occur
after execution, so concurrent attempts must not share those resources. Ordinary
fresh Runners share a process resource-name counter; captured workspace counters
and namespaces are unchanged. This uniqueness fix does not make shared-context
cleanup safe. The internal provenance/selection helper is separate from execution
and cleanup for a future persistent-unit host; the ordinary CLI scheduler,
shared-unit lifecycle and host failure policies remain to be integrated.


### Native execution ownership (in development)

Native shell steps keep an owned, unreaped group leader until their matrix entry
or preparation recipe finishes. This pins the identity used by cancellation:
groups are untracked before the leader is reaped, including on error and unwind.
Script-pipe writers are stopped and joined on every return path. A background
server may continue into subsequent steps of the same entry; draining that entry
does not signal groups owned by another entry. The entry drains its native
scripts before service/Compose cleanup, and a preparation recipe drains them
before computing artifact digests. This prevents a still-running script from
recreating resources after cleanup or mutating a receipt's artifact snapshot.
Holding completed leaders uses kernel child slots for the entry lifetime, rather than silently narrowing the
accepted step count.

This native-group boundary is not a physical stop proof for an entire execution.
Task-traffic-controller wrappers can own grandchildren in a different group, and
an external executor has its own lifetime contract. Their direct child returning,
a cancellation flag or an empty PID list is insufficient. The future shared host
must consume an explicit drain outcome and retain uncertain ownership; the pure
Driver host-terminal policy and ordinary Runner adapter remain unfinished.

The private attempt outcome keeps the actual task `Result` separate from cleanup
errors and remaining ownership observations. Legacy task entry points preserve
cleanup warnings and retain the original result inside a final-cleanup error.
Failed removal and port release keep their references for recovery; failure to
validate a replaced captured workspace never authorizes cleanup in its replacement.
A successful detached Docker create must acknowledge a full container ID. Health,
port and removal commands use that ID, including after renaming. A timed-out,
failed or invalid acknowledgement leaves a creation candidate, never permission
to remove the requested name. Retrying removal first queries that exact ID;
absence is success, and unexpected IDs cannot expand deletion scope.

Generated and rewritten literal Compose namespaces still follow the existing
Runner naming scheme. An external `COMPOSE_PROJECT_NAME` or an unrewritten
script literal is an unverified reference: automatic cleanup will not query or
bring down that project. This
conservative boundary can leave resources for explicit host reconciliation.
The next required integration must assign and attest resources per run/unit,
keep shared setup resources alive for the unit, and map actual engine-domain
stop evidence into the persistent host. The private outcome does not supply a
`ConfirmedStopped` capability, and an empty tracking registry never supplies one.
