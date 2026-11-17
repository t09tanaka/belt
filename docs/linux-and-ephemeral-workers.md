# Linux workers and ephemeral workers

Two ways to run a worker that is not a macOS LaunchAgent. Both use the same
foreground worker role and the same `worker --stdio` bridge the parent reaches
over SSH.

## Linux: systemd user service

The worker-host helper (`worker-service install | start | stop | drain | resume |
status | upgrade | rollback | commit`) drives a systemd user unit on Linux and a
LaunchAgent on macOS; the install journal, the version directories and the
upgrade order are the same.

- The unit is written to `$XDG_CONFIG_HOME/systemd/user/` (default
  `~/.config/systemd/user/`) as `<label>.service`, running
  `worker --input <retained input>` with `Restart=always`.
- `start` runs `systemctl --user daemon-reload`, `enable`, then `start`; `stop`
  asks the worker to drain and finish (or cancel) first, then runs
  `systemctl --user stop`. A stopped unit stays enabled and comes back at the
  next login, as a booted-out LaunchAgent does.
- The helper must run in a session that can reach the user's systemd manager
  (`XDG_RUNTIME_DIR` set, or `/run/user/<uid>` present).
- To keep the worker running after the last login session ends, enable
  lingering for the user once: `loginctl enable-linger <user>`.
- A host without systemd (a container) is not a resident worker host; use an
  ephemeral worker there.

## Ephemeral worker (notebook)

An ephemeral worker lives only while the process runs. It installs no service,
the parent never installs or upgrades it, and it opens no port: the parent
connects over SSH through a tunnel you provide, with `ephemeral = true` on the
worker's `[[workers]]` entry.

Place the `remote-task-runner` binary and the cluster key on the notebook host
yourself (keep the key out of cell output and version control), then start the
worker from a cell:

```
!chmod 700 ./remote-task-runner
!chmod 600 ./cluster.key
!nohup ./remote-task-runner worker --ephemeral \
    --cluster <CLUSTER_ID> --name <WORKER_NAME> \
    --key-file "$PWD/cluster.key" > worker.log 2>&1 &
```

- `--name` is the worker name the parent registered (`id` of its
  `[[workers]]` entry). `--cluster` is the cluster id.
- Each start has its own identity: the worker name stays, and the identity it
  reports changes with every start, so a restarted worker is never taken for
  the one before it.
- It starts clean. It refuses to run if worker state already exists in
  `~/.remote-task-runner`, or if a persistent worker is installed on the host.
- Stop it with SIGTERM or by interrupting the cell. It removes its own state
  directory, unless work is still recorded in it (then the state is kept and
  the next start refuses until you deal with it).

What is not covered here: the tunnel itself, the parent attaching an
`ephemeral = true` worker, and declaring a vanished worker gone
(`workers forget`). Those are later parts of the same plan row.
