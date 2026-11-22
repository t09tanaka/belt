# S26: Mac parent, two Linux workers and a Linux init member

This is a disposable, real SSH/systemd harness, not a simulated worker test.
It runs a native Mac parent and three `linux/arm64` Docker Desktop containers:
w1 and w2 stay worker-only, and m1 becomes an `init` member (its own resident,
parent and worker, as a lingering systemd user service).
The parent uses the hidden foreground `belt daemon --input` entry point and a
private bootstrap (the same contract used by `tests/it/remote_links.rs`). It
never calls Mac `belt init` or registers a LaunchAgent; the public `init`
service-registration path is exercised on Linux only, by m1.

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

For the two-submitter and group-key checks, use a fresh `up.sh` and pass
`--peers` to `run.sh`. The host and m1 submit concurrently and verify the
run listings on each parent. While m1's service is stopped, the host finishes
work on the same w1 worker. After restarting, m1 collects its original attempt
with a single command start. Group rotation then keeps an active attempt
running, restarts a receiving-only worker with its new key, and restores a
skipped peer with `send-key`. Finally, `--remove w2` checks the removal guidance
and the remaining SSH access notices from both `group` and `doctor`.
The per-check results are in `run/evidence/peer-summary.json`.

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
| M1 init member | The host runs `workers add m1 --ssh m1 --install --binary`, then m1 runs `belt init` over SSH as UID 1000 (the spec's order: install, then init, so no `--move`). Exactly one `io.github.t09tanaka.belt.local.*` user unit is active, no worker-only unit is, lingering is on, `bin/resident` names the installed binary's SHA-256 and the unit's main process runs it, `belt group` on m1 reports the host's group, and a host submission placed on m1 succeeds with its command started exactly once. |
| A57 rollback | m1 runs N. A host run is placed on m1, then `belt upgrade --binary <broken N+1>`; once the broken version runs, the upgrade CLI (identified by its pid file, executable and argv) gets SIGTERM, so only the guard can decide. The guard must restore N (`bin/resident`, record removed, main process N) within 90 s, N's directory unchanged, and the host run finishes, started once. Each database contains an original marker before the trial and a changed marker during it. Both must revert. A second task submitted by m1 stays running on w1 across rollback, retains its attempt ID, and is collected with one observed command start. |
| A57 commit | A host run is placed on m1, then `belt upgrade --binary <N+1>` must exit 0. `bin/resident` and the main process name N+1, the old version directory is unchanged (inode, mtime, size, mode, digest), no upgrade record remains, and the run finishes, started once. The upgraded m1 then submits a task to w1 running the older binary and collects its result. |
| A57 trial refusal | m1 submits its own run with `--requires macos` (so it stays unplaced), then runs `upgrade` with the other version directly (what `--binary` delegates to). When the new version runs during the trial, the upgrade process gets SIGSTOP; within 8 s m1's `belt cancel` must be answered `OVERLOADED` and nothing saved; then SIGCONT, the upgrade must commit, and the same cancel must take effect. A missed window fails as incomplete, never PASS. |
| A57 slow drain | A 75-second host run is placed on m1, then the upgrade must wait for it (longer than the 45 s commit deadline) and still commit, with the run started once. |

The A57 checks run in that order (each upgrade toggles m1 between N and N+1)
and SKIP with the reason when the belt on m1 has no `upgrade` subcommand or the
variant binaries are missing; a SKIP is never a PASS. The fixture command takes an
optional marker name and appends one line per start to `~/s26-starts/<marker>`
on the host that runs it, which is how "started once" is counted. A host cancel
of a run on m1 cannot be tested during a trial: the upgrade waits until m1's
worker has no live attempt before it switches, and the hold keeps new ones off
until the commit. The trial-refusal check uses m1's own cancel instead, and runs
of m1's own placed on other members (adopted across the switch) are not covered,
as there is no container-to-container SSH.

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

## A57 against a branch build

`build-linux.sh --upgrade <ref>` exports the Git ref three times with
`git archive` under `linux-cache/src/` (no other worktree is touched) and builds:

- `belt-linux-aarch64`: N, the ref as it is;
- `belt-linux-aarch64-n1`: N+1, the same source with the next patch version in
  `Cargo.toml` and `Cargo.lock` (a different version and SHA-256, which is what
  `belt upgrade` compares);
- `belt-linux-aarch64-n1-broken`: N+1 whose `belt daemon` sleeps forever, so the
  resident runs but never answers health.

The patches are made only in those exports; the product source has no test
knob. The script prints the three digests, and the harness refuses variants that
are not distinct. Build the native Mac parent from the same ref, then run as
usual (uncommitted changes are not in an export):

```bash
REF=feature/r68-upgrade   # any ref with `belt upgrade`
export S26_BASE="$HOME/Library/Caches/belt-s26"
git worktree add --detach "$S26_BASE/src-native" "$REF"
task-traffic-controller exec -- env CARGO_TARGET_DIR="$S26_BASE/native" \
  cargo +1.93.0 build --locked --release -p belt-cli --bin belt \
  --manifest-path "$S26_BASE/src-native/Cargo.toml"
scripts/s26/build-linux.sh --upgrade "$REF"
scripts/s26/up.sh
S26_BELT="$S26_BASE/native/release/belt" scripts/s26/run.sh
scripts/s26/down.sh
git worktree remove "$S26_BASE/src-native"
```

## Scope and expected time

Expect roughly **5–10 minutes** for a warm acceptance run; cold Rust/image builds
can take **10–30+ minutes**, depending on Docker resources and network. These are
estimates, not measured results. The first SSH readiness deadline is 90 seconds.

Not covered: a physical Mac mini / two real Macs (goal 12), Linux x86_64, reboot
or boot ID, suspend/clock/process identity/sleep inhibition, A35 system-user mode,
A34 `workers upgrade`, A37 a real project, or goal 16 multi-submitter/failover.
This proves only the listed parts of A45/A34 if it actually passes. Docker
Desktop's cgroup/systemd behavior remains an environment-specific prerequisite;
there is no fallback to a fake service or ephemeral worker.

Authoring validation: Bash syntax, ShellCheck, and Python syntax can run without
Docker. Real Docker/SSH/systemd acceptance must be run on the host; a sandbox
that denies the Docker socket cannot establish those results.
