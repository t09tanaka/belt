# remote-task-runner

Run CI and development commands across your own Macs on the LAN: one coordinator, many workers, with uncommitted changes, durable queues, cancellation and crash recovery.

Status: in development. The specification (Japanese) is in [docs/spec.md](docs/spec.md), with progress and remaining acceptance work in [docs/plan.md](docs/plan.md).

Builds on [local-ci](https://github.com/t09tanaka/local-ci) for CI planning and [task-traffic-controller](https://github.com/t09tanaka/task-traffic-controller) for per-host resource admission.

## Development

The CLI surface is defined, but public orchestration commands remain under development. Unimplemented commands return `NOT_IMPLEMENTED` (exit code 70; with `--json`, an envelope with `schema_version` on stdout).

Use the current stable Rust toolchain for formatting and linting. The minimum supported compiler is Rust 1.89. Workspace integration tests require fast-worktree 0.4.0 or newer and portool; resource admission uses task-traffic-controller 0.8.2 or newer. Initialize and check the worktree's portool allocation before starting services or integration tests.

Run the configured CI against committed HEAD:

```sh
local-ci run --include-disabled
```

## License

MIT

A separately provisioned persistent worker can run in the foreground with
`remote-task-runner worker --input "$WORKER_INPUT"`. The retained input is private
and binds the cluster, worker identity, executable, key and captured configuration.
Its HOME and canonical worker state must match the `worker --stdio` bridge. The
bridge may disconnect while accepted work and its independent supervisor continue.
The parent snapshots explicitly registered workers from its private user
`config.toml` at startup. Each `[[workers]]` entry's `id` is the expected worker
identity; `ssh` is its SSH destination. The parent verifies cluster, identity and
protocol before attachment, forwards captured source and secrets on that link,
and reconnects without creating a replacement attempt. The remote executable is
`remote-task-runner` on the remote PATH, through the fixed `worker --stdio` bridge.
SSH connection failures stay isolated from local work and other peers.

Registration changes require a parent restart. Automatic enrollment, worker
add/install, ephemeral workers, native worker services and upgrade/rollback remain
required follow-up work. Cluster keys and worker inputs are provisioned privately
out of band; a hello never enrolls an unknown identity.


Worker-link attachment is asynchronous: `Coordinator::attach(...).await` installs
a verified connection after any in-flight placement commit. `detach` and source
session resets use the same placement ordering. Forecasts remain outside this
fence, so a worker may still join during a running tick. Synchronous construction
and cost callbacks can use checked `try_attach`; a busy result installs nothing
and must be retried or handled, never treated as a successful attachment.
