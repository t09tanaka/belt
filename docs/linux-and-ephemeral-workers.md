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
  `doctor` asks each systemd host (`loginctl show-user <uid>
  --property=Linger`) and warns, with that command, when lingering is off.
- A host without systemd (a container) is not a resident worker host; use an
  ephemeral worker there.

### Installing a Linux worker from the parent

`workers add <name> --ssh <dest> --install` and `workers upgrade <name>` send
the parent's own binary only to a host of the parent's own operating system
and architecture. For any other host, build `belt` of the same
version for that host and name the build:

```
belt workers add mini-l --ssh ci@mini-l.local --install \
    --binary target/x86_64-unknown-linux-gnu/release/belt
belt workers upgrade mini-l \
    --binary target/x86_64-unknown-linux-gnu/release/belt
```

- Without `--binary`, a host of another platform is refused before anything is
  sent; the message names the target triple that is needed. Nothing is
  downloaded for you.
- After the copy and before anything is installed or switched, the parent runs
  the copy's `--version` on the host. It must print exactly what the parent
  prints. A copy that does not is removed again, and the installed version is
  left as it was.
- `doctor` compares binaries by digest, so a worker of another platform always
  shows "another build"; upgrade it with `--binary` when the parent's version
  changes.

### Dedicated worker available before login

In the parent's `[[workers]]` entry, set `service_mode = "system-user"` and use
the non-root build account for SSH. Running `workers add NAME --ssh DEST --install`
stages the worker under that account and reports the configuration directory
and the expected system service file. Supply `--binary` as above for another
platform.

On the worker host, an administrator registers the system definition with an
administrator-controlled copy of the same CLI. For a Linux account named
`build`, an example is

```sh
sudo /usr/local/sbin/belt register-system-worker \
    --account build --config-dir /home/build/.config/belt
```

Use the configuration directory reported by the SSH helper. The command prints
the installed definition's full path. On Linux, run `systemctl daemon-reload`
and `systemctl enable --now <printed-unit-filename>` with administrator rights.
On macOS, run `launchctl bootstrap system <printed-plist-path>` with
administrator rights. Repeat the parent's install command to verify the
running worker and finish enrollment.

The system definition starts the non-root account at boot and follows the
worker's installed version across upgrades. On Linux the unit must be enabled
for boot. The helper checks the unit's effective user, activation and drop-ins.
Start and stop use the system manager. The SSH account needs host authorization
for those operations on its own unit, including the stop and start performed
by upgrades. Keep the administrator's registration binary and system definition
under administrator ownership. `doctor` checks the configured service mode and
the worker's service environment. Linux system services run independently of
user-session lingering.

### The coordinator on Linux

`init` registers the coordinator (with its local worker) as a systemd user
unit on Linux, in the same directory as a worker's unit. The same note about
lingering applies, and `doctor` checks it for the parent too.

## Ephemeral worker (notebook)

An ephemeral worker lives only while the process runs. It installs no service,
the parent never installs or upgrades it, and it opens no port: the parent
connects over SSH through a tunnel you provide, with `ephemeral = true` on the
worker's `[[workers]]` entry.

Place the `belt` binary and the cluster key on the notebook host
yourself (keep the key out of cell output and version control), then start the
worker from a cell:

```
!chmod 700 ./belt
!chmod 600 ./cluster.key
!nohup ./belt worker --ephemeral \
    --cluster <CLUSTER_ID> --name <WORKER_NAME> \
    --key-file "$PWD/cluster.key" > worker.log 2>&1 &
```

- `--name` is the worker name the parent registered (`id` of its
  `[[workers]]` entry). `--cluster` is the cluster id.
- Each start has its own identity: the worker name stays, and the identity it
  reports changes with every start, so a restarted worker is never taken for
  the one before it.
- It starts clean. It refuses to run if a persistent worker is installed on
  the host (a worker service, or a parent set up with `belt init`), or if
  `~/.belt` holds state that an ephemeral worker did not create (no
  `ephemeral.json` in it).
- Stop it with SIGTERM or by interrupting the cell. It removes its own state
  directory, unless work is still recorded in it (then the state is kept).
- If it was killed (SIGKILL, a container that was killed, power loss), its
  state stays in `~/.belt`. The next `belt worker --ephemeral` clears it by
  itself and starts under a new identity, once nothing uses it any more: the
  earlier worker, its supervisors and every command it started are gone. It
  knows this from `ephemeral.lock`, which all of them hold. While one of
  them still runs, the start exits with 1, says which condition failed,
  removes nothing and signals nothing; stop that process (or wait for it)
  and start again. A command that started a long-lived helper (a build
  daemon, for example) keeps the lock held until that helper is gone.
- This self-clearing needs `~/.belt` on a local filesystem (APFS, HFS+, ext4,
  xfs, btrfs, zfs, tmpfs, overlayfs). On a network filesystem or one shared
  between a host and its guest (NFS, SMB, 9p, virtiofs, FUSE; a Docker
  Desktop bind mount is one), a lock says nothing about the other machine, so
  the start refuses and you remove the directory yourself. Both sides count:
  the worker writes into `ephemeral.json` what filesystem it saw `~/.belt`
  on, and a later start clears the state only if that was a local one and
  what it sees itself is a local one too. So a state made inside a container
  on a bind mount is not cleared by a start on the host, where the same
  directory looks local.
- A start that was killed in the instant between creating `~/.belt` and
  writing `ephemeral.json` leaves a directory without the marker (empty, or
  with only `ephemeral.lock` and `ephemeral.json.part`). It is refused like
  any state without the marker; remove the directory.
- A persistent worker refuses a state that has `ephemeral.json`, and writes
  nothing into it: `belt worker`, installing or starting the worker service,
  and the parent's own resident. Start `belt worker --ephemeral` once to
  clear it, or remove the directory.

### On the parent

Register the worker with its tunnel's SSH destination and mark it ephemeral:

```
belt workers add colab-1 --ssh colab-1 --ephemeral
```

(or write `ephemeral = true` on its `[[workers]]` entry). The parent then
dials it like any other worker. If the binary is not on the SSH login `PATH`
of the notebook host, add `binary = "/absolute/path/belt"` to
the entry.

- The parent never installs, upgrades, starts, stops, drains or resumes an
  ephemeral worker; those commands are refused without contacting it.
  `doctor` checks whether it answers.
- While the parent has a connection, `belt workers diff colab-1`
  compares the active worker profile with the parent. Use
  `belt workers sync colab-1` to install eligible tool versions
  and measure the result. Both commands need the coordinator resident and the
  notebook worker to be running. A disconnected worker produces a measurement
  error.
- While the worker cannot be reached, the tasks it held stay `unknown`. They
  are not given to another worker and not failed, however long it takes.
- When the same worker is started again, it comes back with a new identity.
  The parent refuses the new identity until you forget the previous one.
  The previous identity can reconnect and report its attempts in the meantime.
  Run `belt workers` to see the registered SSH destination,
  connection state and recorded identity. If the resident cannot answer, the
  connection state is `unknown`. Add `--json` for the same fields as JSON.
- When the previous worker is gone, say so:

  ```
  belt workers forget colab-1
  ```

  Its identity is retired and the tasks it held end as `interrupted`
  (`cancelled` if you had already cancelled them). They are not run again by
  themselves: retry them. A worker that comes back with a retired identity is
  refused. `forget` is refused for a worker that is connected and for a worker
  that is not ephemeral, and it needs the coordinator resident to be running.

What is not covered here: the tunnel itself. Acceptance on real Linux hosts
and through a real tunnel is still to be done.
