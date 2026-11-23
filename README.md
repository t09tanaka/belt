# belt

Run CI and development commands on one machine or share work with other members of your LAN. Each initialized member can submit work. The resident manages queued runs, cancellation and crash recovery.

Status: in development. The specification (Japanese) is in [docs/spec.md](docs/spec.md), with progress and remaining acceptance work in [docs/plan.md](docs/plan.md).

## One install, four tools

You do not need a LAN to use belt. On a single machine, the `belt` binary is a complete local CI and development setup with these four tools linked in:

| Tool | What it does in belt |
|---|---|
| [local-ci](https://github.com/t09tanaka/local-ci) | `belt ci` plans and runs your repository's workflows locally and can post the result as a GitHub commit status. |
| [task-traffic-controller](https://github.com/t09tanaka/task-traffic-controller) | Every command runs under the host's resource admission budget, so parallel builds and tests take turns instead of overloading the machine. |
| fast-worktree | Workers prepare each run's worktree, dependencies included, in seconds. |
| portool | Each worktree gets its own ports and local resources, so runs do not collide. |

Workspace hooks and commands can call `fast-worktree` and `portool` directly. `belt workers diff` shows the version of each linked tool. You do not need to install any of them separately. Embedded fast-worktree and portool are copyright © 2026 Takuto Tanaka and distributed under the [MIT license](LICENSE).

## Install

Build the CLI from a checkout with the stable Rust toolchain. From the repository root, run:

```sh
cargo install --locked --path .
belt --version
```

Cargo installs the executable as `belt` (normally in `~/.cargo/bin`). The package name in this repository is `belt-cli`.

## First run on one machine

### Run in the current directory

On a Mac with no belt coordinator or worker agent running, run a command in your current directory:

```sh
belt exec -- /usr/bin/printf 'hello from belt\n'
```

This prints `hello from belt` on stdout. Belt reports direct execution on stderr and uses the same host admission budget as `task-traffic-controller exec`. A direct command creates no snapshot or run ID.

```sh
belt --json exec --local -- /usr/bin/true
```

The JSON result includes `"mode":"direct"` and `"exit_code":0`. Once a coordinator is running, `--local` pins execution to your own worker and the command follows the normal snapshot and run lifecycle.

### Run with a durable queue

To use the durable queue, initialize the resident for your user, then check its status:

```sh
belt init
belt doctor
```

From a Git project you want to run a command in, try:

```sh
belt exec -- pwd
```

With no remote workers configured, the local worker runs the command. Use `belt top` in a second terminal to follow longer jobs.

## Watch workers and tasks

Run `belt top` in a terminal to see worker connections, active attempts, waiting tasks, and attempts whose state is uncertain. Task details show the executable name. Commands wrapped in `portool exec` show the inner executable followed by `[portool]`. The view refreshes every second. Set the period with `--interval 2`, scroll with the arrow keys, and quit with `q`, Esc, or Ctrl-C. Use `belt --json top` for one snapshot in a pipeline.

## Development

The CLI surface is defined, but public orchestration commands remain under development. Unimplemented commands return `NOT_IMPLEMENTED` (exit code 70; with `--json`, an envelope with `schema_version` on stdout).

Use the current stable Rust toolchain for formatting and linting. The minimum supported compiler is Rust 1.89. Workers include fast-worktree and portool. Workspace hooks and commands use the versions shown by `belt workers diff`. The coexistence tests also exercise standalone installations. Resource admission uses task-traffic-controller 0.10.x (0.10.2 or newer in that minor: the worker shares the ledger of the same user's CLI, and another minor may change its format). The worker also reads that CLI's configuration file, so a key or value the 0.10 library rejects there fails every command the worker starts until the file is corrected. Initialize and check the worktree's portool allocation before starting services or integration tests.

Run the configured CI against committed HEAD:

```sh
local-ci run --include-disabled
```

Pure mutation and line coverage share `scripts/pure-modules.txt`. Mutation uses
`python3 scripts/mutation-run.py --shard 0/4` (repeat shards 1–3), selecting
module library tests plus the existing `config::` integration tests. Shared
toolchain parser tests live under `toolchain::tests::`; substring matches also
include a few library consumers such as `service::outputs::`. Actual integration
discovery fails if any selected test is outside `config::`. Normal CI on both
platforms and the coverage gate still execute the full test suite.

The runner pins cargo-schemata 0.6.0, limits baseline/test execution to 60 seconds
per binary/mutant (compilation is separate), and rejects missing, empty, surviving,
timed-out or unknown results. `python3 scripts/mutation-policy-test.py` checks the
real runner's Cargo arguments and rejects workflow bypasses and accidental extra
integration tests. Each shard uploads selected test names, `selection.json`
(run identity, counts and timings), `run.log` and the mutation report. Use the
repository's resource admission and portool entry when running locally.

## Distributed test jobs

`belt ci plan` identifies jobs that a worker can enumerate.
`belt ci plan --run <run-id>` reads the stored plan, including
the shard task IDs, test counts and reasons a job stayed whole. Discovery runs against the
captured source with the job's setup. After the parent receives the listing,
it releases the tests to run and keeps each shard's identity for retries.

Playwright and Cypress projects can declare their independent fixtures for
a specific job in the captured `.belt.toml`. Set `allow_retry`
when running a failed shard again is safe for the external resources it uses.

```toml
[[ci.sharding]]
workflow = ".github/workflows/browser.yml"
job = "browser"
isolation = "Each attempt creates a private database and browser profile."
allow_retry = true
```

## License

MIT

A separately provisioned persistent worker can run in the foreground with
`belt worker --input "$WORKER_INPUT"`. The retained input is private
and binds the cluster, worker identity, executable, key and captured configuration.
Its HOME and canonical worker state must match the `worker --stdio` bridge. The
bridge may disconnect while accepted work and its independent supervisor continue.
The parent snapshots explicitly registered workers from its private user
`config.toml` at startup. Each `[[workers]]` entry's `id` is the expected worker
identity; `ssh` is its SSH destination. The parent verifies cluster, identity and
protocol before attachment, forwards captured source and secrets on that link,
and reconnects without creating a replacement attempt. The remote executable is
`belt` on the remote PATH, through the fixed `worker --stdio` bridge.
SSH connection failures stay isolated from local work and other peers.

Registration changes require a parent restart. Automatic enrollment, worker
add/install and a Linux parent remain required follow-up work. The Linux systemd
user service backend and `worker --ephemeral` exist on the worker side; see
`docs/linux-and-ephemeral-workers.md`. Cluster keys and worker inputs are provisioned privately
out of band; a hello never enrolls an unknown identity.


Worker-link attachment is asynchronous: `Coordinator::attach(...).await` installs
a verified connection after any in-flight placement commit. `detach` and source
session resets use the same placement ordering. Forecasts remain outside this
fence, so a worker may still join during a running tick. Synchronous construction
and cost callbacks can use checked `try_attach`; a busy result installs nothing
and must be retried or handled, never treated as a successful attachment.
