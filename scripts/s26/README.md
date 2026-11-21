# S26: Mac parent and two Linux workers

This is a disposable, real SSH/systemd harness, not a simulated worker test.
It runs a native Mac parent and two `linux/arm64` Docker Desktop containers.
The parent uses the hidden foreground `belt daemon --input` entry point and a
private bootstrap (the same contract used by `tests/it/remote_links.rs`). It
never calls Mac `belt init` or registers a LaunchAgent. It does **not** test the
public `init` service-registration path.

## Prerequisites and run

Use a short checkout path on an Apple Silicon Mac: macOS Unix socket paths
must fit in 104 bytes. Install/start Docker Desktop with privileged containers
and writable cgroups allowed. Host tools: Bash, Python 3.9+, Git, `/usr/bin/ssh`,
`ssh-keygen`, `portool` (repository baseline 0.10.2), `task-traffic-controller`, and native
`fast-worktree` **0.4.0**. Scripts resolve the local CLIs from PATH, then
`~/.cargo/bin`. Missing prerequisites fail explicitly.

From the repository root, with Rust **1.93.0** already installed:

```bash
# Build the native parent (or supply an existing native build of this checkout).
task-traffic-controller exec -- env CARGO_TARGET_DIR="$PWD/target/s26/native" \
  cargo +1.93.0 build --locked --release -p belt-cli --bin belt
scripts/s26/build-linux.sh
scripts/s26/up.sh
S26_BELT="$PWD/target/s26/native/release/belt" scripts/s26/run.sh
# Inspect target/s26/run/evidence/ before cleanup.
scripts/s26/down.sh
```

The Linux build uses the official `rust:1.93.0-bookworm` image, with Cargo
downloads and artifacts in `target/s26/linux-cache/`; output is
`target/s26/belt-linux-aarch64`. No host cross-compiler is used. The worker
image uses `debian:12.9-slim`, real systemd PID 1, sshd/PAM, a lingering UID 1000
`belt` user, Git, shell/core utilities and `fast-worktree` 0.4.0. The latter is
required by `src/workspace/fastwt.rs`; Git and core utilities serve source and
workspace operations. Rust/tool image tags and installed Rust tool versions
are pinned; Debian apt security packages are intentionally not snapshot-pinned.
The harness has no Docker socket mount in workers.

`up.sh` uses portool sync/check/exec against a harness-owned Git resource root
under `target/s26/resources/`. It keeps generated configuration there and uses
the normal shared portool ledger, so its localhost SSH ports do not collide
with other portool allocations. It does not modify the product checkout's
portool configuration or Git hooks. Container/network names incorporate the
portool identity and a fresh run token. Builds and the finite test runner use
task-traffic-controller; do not wrap the scripts a second time.

Each `up` generates its own SSH key, config, known_hosts and `bin/ssh` wrapper.
The wrapper places `-F` before belt's `--`. Global known_hosts and the SSH agent
are disabled. No real `~/.ssh`, `~/.belt` or `~/.config/belt` is read or written.
The parent gets an allowlisted environment with private HOME, XDG config/state,
TMPDIR and PATH; its only non-system helper is the pinned fast-worktree binary.
No host sudo is used. A tiny Git repository and fixture commit are generated
inside the run directory; the product repository is never committed or pushed.

## PASS criteria

The checks deliberately run in this order, so w2 joins an existing backlog:

| Check | Required evidence |
| --- | --- |
| A02 | Install only w1 using `workers add --ssh --install --binary`; submit 12 host-agnostic 90-second shell jobs. Observe active attempts on local and w1 plus tasks with **no attempts**. Save those task IDs, install w2, and require at least one of those exact tasks to finish successfully on w2. All 12 must finish and collect successfully. |
| A45, part | Both installs occurred above; SSH verifies systemd PID 1, UID 1000, lingering and `systemctl --user is-active` on exactly one installed belt worker unit. `belt doctor` must exit successfully, report healthy, and contain checks for both workers with no failure or unverified result. |
| A01 | Submit nine more 10-second shell jobs with no host selectors. `belt wait` must succeed for all; `belt show --json` must report one successful, completely collected attempt per task and the worker-ID set exactly `local`, `w1`, `w2`. |
| A34, part | `belt workers stop w2` followed by SSH user-service state `inactive` (exit 3); `belt workers start w2` followed by state `active` (exit 0). SSH failures cannot count as stopped. |

The parent's admission window is one active task per worker with zero prefetch.
Worker policy is not patched. Every belt command's stdout/stderr, before/after
placement snapshots, parent log and `summary.json` are retained in
`target/s26/run/evidence/`. A row prints PASS or FAIL with its reason; any failure
returns nonzero. Setup failures report an incomplete harness, never acceptance.
Checks have bounded waits (45 seconds for service transitions, up to ten minutes
for a submitted batch). Failures do not silently retry submission or clear state.

`run.sh` owns the foreground parent process and stops it on exit/interrupt; it
requests cancellation of submitted tasks before shutdown. Do not SIGKILL the
runner. Stop an active runner with Ctrl-C before `down.sh`: a lock prevents
cleanup racing a live runner. `down.sh` removes only containers/networks bearing
this run's random label, then the private run directory. Docker errors preserve
the directory for a retry. It never prunes Docker resources. Build caches, the
worker image, and the reusable portool resource allocation remain for reruns;
down does not change the shared portool ledger. Repeat with down/up/run, not by
running `run.sh` twice against the same parent state.

## Scope and expected time

Expect roughly **5–10 minutes** for a warm acceptance run; cold Rust/image builds
can take **10–30+ minutes**, depending on Docker resources and network. These are
estimates, not measured results. The first SSH readiness deadline is 90 seconds.

Not covered: a physical Mac mini / two real Macs (goal 12), Linux x86_64, reboot
or boot ID, suspend/clock/process identity/sleep inhibition, A35 system-user mode,
A34 upgrade/rollback, A37 a real project, or goal 16 multi-submitter/failover.
This proves only the listed parts of A45/A34 if it actually passes. Docker
Desktop's cgroup/systemd behavior remains an environment-specific prerequisite;
there is no fallback to a fake service or ephemeral worker.

Authoring validation: Bash syntax, ShellCheck, and Python syntax can run without
Docker. Real Docker/SSH/systemd acceptance must be run on the host; a sandbox
that denies the Docker socket cannot establish those results.
