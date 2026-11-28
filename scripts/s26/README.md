# S26: Mac parent, two Linux workers and a Linux init member

This is a disposable, real SSH/systemd harness, not a simulated worker test.
It runs a native Mac parent and three Linux Docker Desktop containers (arm64 by default):
w1 and w2 stay worker-only, and m1 becomes an `init` member (its own resident,
parent and worker, as a lingering systemd user service).
The parent uses the hidden foreground `belt daemon --input` entry point and a
private bootstrap (the same contract used by `tests/it/remote_links.rs`). It
never calls Mac `belt init` or registers a LaunchAgent; the public `init`
service-registration path is exercised on Linux only, by m1.

For x86_64 workers, export `S26_ARCH=amd64` before building, starting,
running and cleaning up. This selects a native x86_64 compiler inside Docker,
the `belt-linux-x86_64` artifacts and an architecture-specific image. The
default base is `target/s26-amd64`; if setting `S26_BASE` explicitly, give
each architecture its own base so the workers and evidence stay separate.

## Prerequisites and run

Use a short checkout path on an Apple Silicon Mac: macOS Unix socket paths
must fit in 104 bytes. Install/start Docker Desktop with privileged containers
and writable cgroups allowed. Host tools: Bash, Python 3.9+, Git, `/usr/bin/ssh`,
`ssh-keygen`, `portool` (repository baseline 0.10.2), and `task-traffic-controller`. Scripts resolve the local CLIs from PATH, then
`~/.cargo/bin`. Missing prerequisites fail explicitly.

From the repository root, with Rust **1.93.0** already installed:

```bash
# Build the native parent (or supply an existing native build of this checkout).
task-traffic-controller exec -- env CARGO_TARGET_DIR="$PWD/target/s26/native" \
  cargo +1.93.0 build --locked --release -p belt-ci --bin belt
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
`belt` user, Git and shell/core utilities. The app supplies fast-worktree and
portool for workspace hooks and commands. Rust/tool image tags and installed Rust tool versions
are pinned; Debian apt security packages are intentionally not snapshot-pinned.
Workers run with privileged systemd and share the Docker VM kernel. The worker
image masks `systemd-binfmt.service` and its automount so that shutting down a
worker preserves the VM's cross-architecture executable handlers. If an amd64
build fails before the first Dockerfile command, check the runtime separately
with `docker run --rm --platform linux/amd64 debian:12.9-slim dpkg --print-architecture`.
It should print `amd64`. The harness has no Docker socket mount in workers.

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
TMPDIR and PATH. Its submitted jobs run the embedded fast-worktree and portool commands.
No host sudo is used. A tiny Git repository and fixture commit are generated
inside the run directory; the product repository is never committed or pushed.

## PASS criteria

The checks deliberately run in this order, so w2 joins an existing backlog:

| Check | Required evidence |
| --- | --- |
| A02 | Install only w1 using `workers add --ssh --install --binary`; submit 12 host-agnostic 90-second shell jobs. Observe active attempts on local and w1 plus tasks with **no attempts**. Save those task IDs, install w2, and require at least one of those exact tasks to finish successfully on w2. All 12 must finish and collect successfully. |
| A45, part | Both installs occurred above; SSH verifies systemd PID 1, UID 1000, lingering and `systemctl --user is-active` on exactly one installed belt worker unit. Each worker has its own non-empty `/etc/machine-id` (the sweep identifies a machine by it). `belt doctor` must contain checks for both workers; no check on any host fails or is `not_checked`, except `environment_diff`, which fails by design between a macOS parent and Linux workers. |
| A01 | Submit nine more 10-second shell jobs with no host selectors. `belt wait` must succeed for all; `belt show --json` must report one successful, completely collected attempt per task and the worker-ID set exactly `local`, `w1`, `w2`. |
| A34 | Stop and start w2, switch to a second binary digest, interrupt its bridge health probe and verify rollback. The rollback must leave the original executable's inode, mode, size and digest unchanged. Retry successfully; the commit removes the previous version's directory, which nothing names any more (spec chapter 25, stage 4); a repeated upgrade reports `already_current`; then the original build is installed again. |
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
`down.sh --all` does the same and then gives the portool allocation back
(`portool release`) and removes the build caches (`linux-cache`, `native`),
the built Linux binaries, `resources` and the worker image of the current
`S26_ARCH`. Pulled base images and `harness.lock` stay.

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
  cargo +1.93.0 build --locked --release -p belt-ci --bin belt \
  --manifest-path "$S26_BASE/src-native/Cargo.toml"
scripts/s26/build-linux.sh --upgrade "$REF"
scripts/s26/up.sh
S26_BELT="$S26_BASE/native/release/belt" scripts/s26/run.sh
scripts/s26/down.sh
git worktree remove "$S26_BASE/src-native"
```

## Linux parent (`--linux-parent`)

A Linux container is the parent and another is its worker; no native Mac binary
is needed (`S26_BELT` is not read). On a fresh `up.sh`, after `build-linux.sh`:

```bash
scripts/s26/run.sh --linux-parent
scripts/s26/down.sh
```

m1 is initialized with `belt init --name m1` (a lingering systemd user
service). It reaches w1 over a test-only SSH key generated inside the
containers (the host's `~/.ssh` is untouched), then runs `belt workers add w1
--ssh w1 --install`. Everything is driven through m1's own installed binary
over SSH. Scenarios, each PASS/FAIL/SKIP (a SKIP is not a PASS and fails the
run):

| Check | Required evidence |
| --- | --- |
| L01 | `belt init` on Linux: one active init unit, lingering, `bin/resident` names the installed digest, `belt group` answers |
| L02 | `workers add w1 --install` from the Linux parent; w1 connected |
| L03 | A submitted run is placed on w1; its result is collected (one complete, succeeded attempt, command started once) and `belt logs` shows w1's output |
| L04 | `belt exec` from the parent, with the parent's own worker busy, runs on w1 (attempt worker and `belt logs` output) |
| L05 | `belt cancel` ends a run on w1; the attempt is not succeeded and nothing keeps running there |
| L06 | `belt doctor` exits 0 and reports healthy, with checks for w1; none fails or is `not_checked` |
| L07 | The parent's service is stopped while a run is on w1, then started; the same attempt is collected, command started once |

Evidence: `run/evidence/linux-parent-summary.json` and the `*-lp-*` files.

## Scope and expected time

Expect roughly **5–10 minutes** for a warm acceptance run; cold Rust/image builds
can take **10–30+ minutes**, depending on Docker resources and network. These are
estimates, not measured results. The first SSH readiness deadline is 90 seconds.

On arm64 Docker workers, A34 recovered the original worker after a failed
post-switch SSH health check. A second upgrade succeeded and its repeat
returned `already_current`. In the dedicated A35 run, the system unit
remained enabled, its PID changed after restart, the parent completed the
bridge handshake and the non-root worker ran a job. The A37 timings below
record the separate project CI comparison.

Physical Mac startup, suspend and boot identity still need hardware.
Docker Desktop runs the containers on a shared Linux kernel.

Authoring validation: Bash syntax, ShellCheck, and Python syntax can run without
Docker. Real Docker/SSH/systemd acceptance must be run on the host; a sandbox
that denies the Docker socket cannot establish those results.

## Compare a project's CI on one and multiple workers

On a fresh `up.sh` allocation, set `S26_PROJECT_CI` to the absolute path of a
trusted local Git checkout and use the normal `run.sh` entry. This mode clones
its committed HEAD into the private harness directory. It executes that commit's
CI twice on the local worker, installs w1 and w2, then executes it twice more.
The project must have independent jobs that can run on both platforms, with
their tools available in the harness environments.

Use `run/evidence/project-ci.json` to compare elapsed time, placement and
collection timestamps. The first and repeat measurements on each topology are
reported separately. The local worker keeps its cache when the other workers
join. A distributed run must place work on at least two workers and return the
same job outcomes as the local baseline. Copy the evidence before `down.sh`.

### Project comparison measured on 2026-11-22

The workload was the 15 pytest tests in `t09tanaka/project-backlog` at
`f5fb10b75002ee16049296ead798cf78d93410cf`. Its checkout had no CI workflow.
A private comparison commit (`7eae2c56abff378920798dfc8d39fc720e0895bf`)
added four independent jobs, one per test module, and vendored pytest 7.4.4
and its dependencies. All four runs below used that same commit. Each job
ran `PYTHONPATH=vendor python3 -m pytest -q tests/test_<module>.py`.
The parent was macOS and the w1/w2 workers were arm64 Linux containers on
the same machine.

The native timing build was `15aef9e` on macOS 27.0.1 arm64. The first
parent run started with a fresh allocation. The group inherited the parent's
cache and added two fresh arm64 Debian workers over SSH; the resident stayed
running throughout. Each topology ran the same four independent jobs twice.

| Configuration | Elapsed (s) | Preparation (s) | Input delivery (s) | Resource wait (s) | Work (s) | Collection (s) | Workers executing jobs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Parent, first | 25.86 | 9.065 | 8.136 | 0.035 | 5.624 | 0.768 | local |
| Parent, repeat | 16.84 | 7.924 | 0.000 | 0.037 | 5.054 | 0.891 | local |
| Group, first | 13.20 | 5.846 | 3.334 | 0.053 | 5.409 | 1.412 | local, w1 |
| Group, repeat | 12.70 | 11.495 | 0.002 | 0.293 | 5.182 | 2.873 | local, w1, w2 |

All four job outcomes matched on every run. The first-run elapsed-time ratio
was 1.96× and the repeat ratio was 1.33×. Preparation spans worker acceptance
to command registration. Delivery includes source forwarding and frozen-plan
delivery, measured on the parent. Cached source delivery takes zero forwarding
time. Resource wait and work come from the collected job artifacts. Collection
spans terminal status to closed collection. These phase totals sum observations
across jobs or workers that can overlap; elapsed time is measured once per run.
A physical LAN and four independent cold worker caches were outside this sample.

## Dedicated system service

On a fresh allocation, set `S26_SYSTEM_WORKER=1` for both `up.sh` and
`run.sh`. This case uses w1 only. It stages the worker as the SSH user,
registers the unit with a trusted administrator copy of belt inside w1,
enables it in the system manager and finishes enrollment from the parent.
A job must then complete on that worker while its service runs as the
non-root belt account. The evidence is in `run/evidence/system-worker.json`.

## A46 ephemeral worker over SSH

Use a fresh `up.sh` run for `S26_BELT=/absolute/path/to/belt scripts/s26/run.sh --a46`. This check uses the run's disposable w1 guest as an ephemeral worker and connects through an OpenSSH local forward. It removes that forward while a task runs, observes one unknown attempt, runs `workers forget`, and reconnects the same live agent. The command must have started once, its original attempt must be interrupted, and the old agent must receive the retirement notice. A second worker start must have a new instance identity. See `run/evidence/a46.json` and the two agent logs before running `down.sh`.

Verified on 2026-11-22 with a Mac coordinator and a Debian arm64 Docker guest. The command started once. The same attempt changed from unknown to interrupted on explicit forget, and the returning worker exited after receiving the retirement notice. A subsequent start produced a distinct instance ID. Native SHA-256: `ff6c2cdb4830838b7f79f4850764926b86c484df841ef623fa19efe67f7417d0`. Linux SHA-256: `40a16389ca2a4522657ee6823237f24e04e3b9f453453ca169578bc35fe3fa6e`.
