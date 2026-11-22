#!/usr/bin/env python3
"""Docker/SSH acceptance driver; all belt calls use a private environment."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(os.environ["S26_ROOT"])
BASE = Path(os.environ["S26_BASE"])
LINUX_BINARY = "belt-linux-" + os.environ["S26_MACHINE"]
RUN = Path(os.environ["S26_RUN"])
LABEL = "io.belt.s26.run"
# w1 and w2 stay worker-only (installed by the host); m1 becomes an init member.
HOSTS = ("w1", "w2", "m1")
USER_BUS = ('export XDG_RUNTIME_DIR=/run/user/1000; '
            'export DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus; ')
CONFIG = """schema_version=1
[coordinator]
heartbeat_seconds=1
stale_seconds=10
max_active_task=1
[scheduler]
prefetch_per_worker=0
"""


def command(args, *, env=None, cwd=None, timeout=120, check=True):
    p = subprocess.run([str(a) for a in args], env=env, cwd=cwd,
                       text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       timeout=timeout)
    if check and p.returncode:
        raise RuntimeError(f"{args[0]} {args[1:3]} exited {p.returncode}: "
                           f"{p.stderr[-3000:]} {p.stdout[-3000:]}")
    return p


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")
    path.chmod(0o600)


def until(description, fn, seconds=90):
    deadline = time.monotonic() + seconds
    while True:
        result = fn()
        if result:
            return result
        if time.monotonic() >= deadline:
            raise RuntimeError(f"timeout: {description}")
        time.sleep(1)


def ssh(host, script, check=True, timeout=30):
    # Always use our -F wrapper, never the caller's SSH configuration.
    return command([RUN / "bin/ssh", "-T", "--", host, script],
                   timeout=timeout, check=check)


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class Skip(Exception):
    """A check that cannot run with this build; never counted as PASS."""


def up():
    RUN.mkdir(mode=0o700)
    (RUN / "bin").mkdir()
    token = secrets.token_hex(16)
    prefix = "s26-" + os.environ["PORTOOL_WORKTREE_ID"][:12] + "-" + token[:8]
    manifest = {"token": token, "network": prefix,
                "containers": [prefix + "-" + h for h in HOSTS]}
    # Written first so even a partial up is removable.
    save(RUN / "resources.json", manifest)
    command(["ssh-keygen", "-q", "-t", "ed25519", "-N", "",
             "-f", RUN / "id_ed25519"])
    config = ["Host *", "  User belt", "  HostName 127.0.0.1",
              f'  IdentityFile "{RUN}/id_ed25519"',
              f'  UserKnownHostsFile "{RUN}/known_hosts"',
              "  GlobalKnownHostsFile /dev/null", "  StrictHostKeyChecking accept-new",
              "  IdentitiesOnly yes", "  IdentityAgent none", "  BatchMode yes",
              "  ConnectTimeout 5", "  ConnectionAttempts 1",
              "  ServerAliveInterval 5", "  ServerAliveCountMax 3"]
    for host in HOSTS:
        config += [f"Host {host}", f"  Port {os.environ['S26_' + host.upper() + '_PORT']}"]
    (RUN / "ssh_config").write_text("\n".join(config) + "\n")
    wrapper = RUN / "bin/ssh"
    wrapper.write_text("#!/usr/bin/env bash\nset -euo pipefail\nexec /usr/bin/ssh -F "
                       + shlex.quote(str(RUN / "ssh_config")) + ' "$@"\n')
    wrapper.chmod(0o700)
    command(["docker", "network", "create", "--label", f"{LABEL}={token}", prefix])
    for host, name in zip(HOSTS, manifest["containers"]):
        port = os.environ["S26_" + host.upper() + "_PORT"]
        command(["docker", "run", "-d", "--platform", "linux/" + os.environ["S26_ARCH"],
                 "--privileged", "--cgroupns=host", "--tmpfs", "/run",
                 "--tmpfs", "/run/lock", "--tmpfs", "/tmp",
                 "-v", "/sys/fs/cgroup:/sys/fs/cgroup:rw",
                 "--label", f"{LABEL}={token}", "--name", name,
                 "--hostname", host, "--network", prefix,
                 "-p", f"127.0.0.1:{port}:22",
                 os.environ["S26_IMAGE"]])
        command(["docker", "cp", RUN / "id_ed25519.pub",
                 f"{name}:/home/belt/.ssh/authorized_keys"])
        command(["docker", "exec", name, "sh", "-ec",
                 "chown belt:belt /home/belt/.ssh/authorized_keys; "
                 "chmod 600 /home/belt/.ssh/authorized_keys"])
        until(f"{host}: SSH and lingering user manager", lambda: ssh(
            host, 'test "$(cat /proc/1/comm)" = systemd && '
            'test "$(loginctl show-user 1000 -p Linger --value)" = yes && '
            'test -S /run/user/1000/bus', check=False).returncode == 0)
    print(f"w1, w2 and m1 ready. Evidence and private SSH files: {RUN}")


class Checks:
    def __init__(self):
        binary = Path(os.environ["S26_BELT"])
        if not binary.is_absolute() or not binary.is_file():
            raise RuntimeError("S26_BELT must name an existing absolute native binary")
        if not (BASE / LINUX_BINARY).is_file():
            raise RuntimeError("run build-linux.sh first")
        if len(os.fsencode(RUN / "state/parent/request.sock")) >= 104:
            raise RuntimeError("checkout path is too long for macOS Unix sockets; use a shorter checkout path")
        if (RUN / "home").exists():
            raise RuntimeError("run.sh is single-use per up; down.sh then up.sh to repeat")
        self.binary = RUN / "bin/belt"
        shutil.copy2(binary, self.binary)
        fw = Path(os.environ["S26_FAST_WORKTREE"])
        if command([fw, "--version"]).stdout.strip() != "fast-worktree 0.4.0":
            raise RuntimeError("native fast-worktree 0.4.0 is required")
        (RUN / "bin/fast-worktree").symlink_to(fw)
        for name in ("home", "config/belt", "state", "local", "tmp", "evidence", "project"):
            (RUN / name).mkdir(parents=True, mode=0o700)
        self.env = {"HOME": str(RUN / "home"), "XDG_CONFIG_HOME": str(RUN / "config"),
                    "XDG_STATE_HOME": str(RUN / "home/.local/state"),
                    "TMPDIR": str(RUN / "tmp"), "PATH": f"{RUN}/bin:/usr/bin:/bin:/usr/sbin:/sbin",
                    "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
                    "LANG": "C", "TOKIO_WORKER_THREADS": "2"}
        (RUN / "config/belt/config.toml").write_text(CONFIG)
        (RUN / "cluster.key").write_text(secrets.token_hex(32) + "\n")
        (RUN / "cluster.key").chmod(0o600)
        self.input = RUN / "config/belt/coordinator.json"
        save(self.input, {"state_dir": str(RUN / "state"),
                          "worker_state_dir": str(RUN / "local"), "worker_id": "local",
                          "cluster_key_path": str(RUN / "cluster.key"),
                          "executable": str(self.binary), "env": self.env,
                          "repo_root": None, "expected_cluster": None,
                          "managed_service": None})
        self.project = RUN / "project"
        # This is a generated fixture repository, never the product checkout.
        for args in (["init", "-q"], ["config", "user.name", "S26 Harness"],
                     ["config", "user.email", "s26@example.invalid"]):
            command(["git", *args], env=self.env, cwd=self.project)
        (self.project / "job.sh").write_text(
            '#!/bin/sh\nset -eu\n'
            'if [ -n "${2:-}" ]; then mkdir -p "$HOME/s26-starts"; '
            'hostname >> "$HOME/s26-starts/$2"; fi\n'
            'printf "s26 start %s\\n" "$(hostname)"\n'
            'sleep "$1"\nprintf "s26 complete %s\\n" "$(hostname)"\n')
        command(["git", "add", "job.sh"], env=self.env, cwd=self.project)
        command(["git", "-c", "commit.gpgsign=false", "commit", "-qm", "S26 fixture"],
                env=self.env, cwd=self.project)
        self.count = 0
        self.tasks = []
        self.parent = None
        self.results = []
        self.m1_ready = False
        self.m1_state = None

    def belt(self, *args, timeout=120):
        if self.parent is not None and self.parent.poll() is not None:
            raise RuntimeError("owned parent exited; refusing automatic detached restart")
        self.count += 1
        p = command([self.binary, "--json", *args], env=self.env, cwd=self.project,
                    timeout=timeout, check=False)
        stem = RUN / "evidence" / f"{self.count:04d}-{args[0]}"
        stem.with_suffix(".stdout.json").write_text(p.stdout)
        stem.with_suffix(".stderr.txt").write_text(p.stderr)
        if p.returncode:
            raise RuntimeError(f"belt {' '.join(args)} exited {p.returncode}; see {stem}*: "
                               + (p.stderr + p.stdout)[-1800:])
        value = json.loads(p.stdout)
        if value.get("ok") is False:
            raise RuntimeError(f"belt returned an error: {value}")
        return value.get("result", value.get("data", value))

    def start(self):
        log = open(RUN / "evidence/parent.log", "w")
        self.parent = subprocess.Popen([str(self.binary), "daemon", "--input", str(self.input)],
                                       env=self.env, cwd=self.project, stdin=subprocess.DEVNULL,
                                       stdout=log, stderr=log)
        log.close()

        def ready():
            if self.parent.poll() is not None:
                raise RuntimeError("parent exited; see evidence/parent.log")
            if not (RUN / "state/parent/request.sock").is_socket():
                return False
            try:
                self.belt("ls")
                return True
            except RuntimeError:
                return False
        until("foreground parent ready", ready, 30)
        # `belt init` would record the group identity; this foreground parent
        # skips init (no LaunchAgent), so record the cluster the parent just
        # created and start it again with it, as init does.
        spec = json.loads(self.input.read_text())
        if spec.get("expected_cluster") is None:
            import sqlite3
            db = RUN / "state" / "coordinator.db"
            with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
                (cluster,) = conn.execute(
                    "SELECT cluster_id FROM coordinator_state WHERE singleton = 1").fetchone()
            spec["expected_cluster"] = cluster
            save(self.input, spec)
            self.parent.terminate()
            self.parent.wait(timeout=60)
            self.parent = None
            self.start()

    def install(self, host):
        self.belt("workers", "add", host, "--ssh", host, "--install", "--binary",
                  str(BASE / LINUX_BINARY), timeout=180)
        self.service(host, "active")

    def service(self, host, expected, kind="worker"):
        # kind "worker": the --install worker unit; "local": the init resident.
        script = (USER_BUS +
                  f'set -- "$HOME"/.config/systemd/user/io.github.t09tanaka.belt.{kind}.*.service; '
                  '[ "$#" -eq 1 ] && [ -f "$1" ] || exit 90; '
                  'systemctl --user is-active "$(basename "$1")"')

        def state():
            p = ssh(host, script, check=False)
            # Inactive is code 3; transport/missing-unit errors are never a pass.
            return p.stdout.strip() == expected and p.returncode == (0 if expected == "active" else 3)
        until(f"{host} {kind} user service {expected}", state, 45)

    def submit(self, seconds, count, marker=None):
        tasks = []
        for _ in range(count):
            receipt = self.belt("submit", "--", "sh", "job.sh", str(seconds),
                                *([marker] if marker else []))
            tasks.append(receipt["task_id"])
            self.tasks.append(receipt["task_id"])
        return tasks

    def views(self, tasks):
        views = {task: self.belt("show", task, "--limit", "64") for task in tasks}
        if any(v["attempts"]["next_cursor"] for v in views.values()):
            raise RuntimeError("unexpected attempt pagination; refusing partial evidence")
        return views

    def finish(self, tasks):
        deadline = time.monotonic() + 600
        for task in tasks:
            self.belt("wait", task, timeout=max(1, deadline - time.monotonic()))
        views = self.views(tasks)
        for task, view in views.items():
            attempts = view["attempts"]["items"]
            if len(attempts) != 1:
                raise RuntimeError(f"{task}: expected one attempt, got {len(attempts)}")
            if (attempts[0]["collection_verdict"] != "complete"
                    or (attempts[0]["termination"] or {}).get("outcome") != "succeeded"):
                raise RuntimeError(f"{task}: collection did not succeed: {attempts[0]}")
        return views

    def a02(self):
        self.install("w1")
        tasks = self.submit(90, 12)

        def backlog():
            views = self.views(tasks)
            workers = {a["worker_id"] for v in views.values()
                       for a in v["attempts"]["items"] if a["state"] == "active"}
            unplaced = [t for t, v in views.items() if not v["attempts"]["items"]]
            if {"local", "w1"} <= workers and unplaced:
                return views, unplaced
            return None
        before, unplaced = until("local and w1 occupied, with unplaced backlog", backlog, 45)
        save(RUN / "evidence/a02-before-add.json", before)
        save(RUN / "evidence/a02-unplaced.json", unplaced)
        self.install("w2")
        after = self.finish(tasks)
        save(RUN / "evidence/a02-after.json", after)
        picked = [t for t in unplaced if after[t]["attempts"]["items"][0]["worker_id"] == "w2"]
        if not picked:
            raise RuntimeError("w2 did not complete any task recorded as unplaced before its addition")
        return f"w2 completed {len(picked)} previously unplaced tasks from the original backlog"

    def a45(self):
        for host in ("w1", "w2"):
            self.service(host, "active")
            p = ssh(host, 'test "$(id -u)" = 1000 && '
                    'test "$(cat /proc/1/comm)" = systemd && '
                    'test "$(loginctl show-user 1000 -p Linger --value)" = yes')
            if p.returncode:
                raise RuntimeError(f"{host}: wrong user, PID 1, or lingering")
        # A macOS parent and Linux workers always differ in platform and tools,
        # so doctor's environment_diff fails by design here (spec: a different
        # OS or architecture needs its own profile; A45 runs with a Linux
        # parent). Every other doctor check of the workers must pass.
        self.count += 1
        p = command([self.binary, "--json", "doctor"], env=self.env, cwd=self.project,
                    timeout=180, check=False)
        stem = RUN / "evidence" / f"{self.count:04d}-doctor"
        stem.with_suffix(".stdout.json").write_text(p.stdout)
        stem.with_suffix(".stderr.txt").write_text(p.stderr)
        report = json.loads(p.stdout).get("result") or {}
        expected = ("environment_diff",)
        for host in ("w1", "w2"):
            checks = [c for c in report.get("checks", []) if c["host"] == host]
            if not checks:
                raise RuntimeError(f"doctor did not report {host}")
            bad = [c for c in checks
                   if c["id"] not in expected and c["level"] == "fail"]
            if bad:
                raise RuntimeError(f"doctor failed on {host}: {bad}")
            unchecked = [c for c in checks
                         if c["level"] == "not_checked" and c["id"] != "traffic_processes"]
            if unchecked:
                raise RuntimeError(f"doctor could not check {host}: {unchecked}")
        others = [c for c in report.get("checks", [])
                  if c["host"] not in ("w1", "w2") and c["level"] == "fail"]
        if others:
            raise RuntimeError(f"doctor failed on the parent: {others}")
        return ("both --install workers active as user services, lingering; doctor passes "
                "every check except the expected cross-OS environment_diff")

    def a01(self):
        views = self.finish(self.submit(10, 9))
        save(RUN / "evidence/a01.json", views)
        workers = {v["attempts"]["items"][0]["worker_id"] for v in views.values()}
        if workers != {"local", "w1", "w2"}:
            raise RuntimeError(f"successful attempt hosts were {sorted(workers)}")
        return "nine host-agnostic submissions succeeded across local, w1 and w2"

    def a34(self):
        self.belt("workers", "stop", "w2")
        self.service("w2", "inactive")
        self.belt("workers", "start", "w2")
        self.service("w2", "active")
        return "workers stop/start changed w2 user service inactive -> active"

    # m1: an init member. The host installs it as a worker, then `belt init`
    # runs there as its user, so its one resident (parent and worker) is a
    # lingering systemd user service in the host's group.

    def m1_container(self):
        manifest = json.loads((RUN / "resources.json").read_text())
        (name,) = [c for c in manifest["containers"] if c.endswith("-m1")]
        return name

    # Root inside m1, for what its user cannot do: belt's processes are not
    # dumpable, so only root reads their /proc/<pid>/exe and signals them.
    # HOME names the belt user's home; the user manager is reached with -M.
    def m1_root(self, script, *args):
        return ["docker", "exec", "-e", "HOME=/home/belt", self.m1_container(),
                "bash", "-c", script, "s26", *args]

    def m1_exec(self, script, *args, timeout=60, check=True):
        return command(self.m1_root(script, *args), timeout=timeout, check=check)

    def m1_belt(self, *args, timeout=120, check=True):
        """belt on m1 over SSH, through the resident's own link."""
        self.count += 1
        script = (USER_BUS + 'cd "$HOME/project" && exec "$HOME/.config/belt/bin/resident/belt" '
                  '--json ' + " ".join(shlex.quote(str(a)) for a in args))
        p = ssh("m1", script, check=False, timeout=timeout)
        stem = RUN / "evidence" / f"{self.count:04d}-m1-{args[0]}"
        stem.with_suffix(".stdout.json").write_text(p.stdout)
        stem.with_suffix(".stderr.txt").write_text(p.stderr)
        if not check:
            return p
        if p.returncode:
            raise RuntimeError(f"m1 belt {' '.join(map(str, args))} exited {p.returncode}; "
                               f"see {stem}*: " + (p.stderr + p.stdout)[-1800:])
        value = json.loads(p.stdout)
        if value.get("ok") is False:
            raise RuntimeError(f"m1 belt returned an error: {value}")
        return value.get("result", value.get("data", value))

    def m1_link(self):
        return ssh("m1", 'readlink "$HOME/.config/belt/bin/resident"').stdout.strip()

    def m1_version_file(self, digest):
        # Identity and content of one version directory's binary.
        return ssh("m1", f'f="$HOME/.config/belt/bin/{digest}/belt"; '
                   'stat -c "%i %Y %s %a" "$f" && sha256sum "$f"').stdout

    def m1_main_exe(self):
        p = self.m1_exec('set -- "$HOME"/.config/systemd/user/io.github.t09tanaka.belt.local.*.service; '
                         'pid=$(systemctl --user -M belt@ show -p MainPID --value "$(basename "$1")"); '
                         '[ "$pid" -gt 1 ] && readlink "/proc/$pid/exe"', check=False)
        return p.stdout.strip() if p.returncode == 0 else None

    def m1_upgraded_away(self):
        return ssh("m1", 'test -e "$HOME/.config/belt/upgrade/record.json"',
                   check=False).returncode == 1

    def starts(self, token):
        # How often the fixture command started for this marker, per host.
        counts = {}
        local = RUN / "home/s26-starts" / token
        if local.exists():
            counts["local"] = len(local.read_text().splitlines())
        for host in HOSTS:
            lines = ssh(host, f'cat "$HOME/s26-starts/{token}" 2>/dev/null || true').stdout.splitlines()
            if lines:
                counts[host] = len(lines)
        return counts

    def place_on_m1(self, seconds, limit=5):
        """Submit host-agnostic jobs one at a time until one is placed on m1
        (the others fill the free workers first); wait until it runs."""
        batch = []
        for _ in range(limit):
            token = secrets.token_hex(6)
            (task,) = self.submit(seconds, 1, marker=token)
            batch.append(task)

            def placed():
                items = self.belt("show", task, "--limit", "64")["attempts"]["items"]
                return items[0] if items else None
            attempt = until(f"{task} placed", placed, 60)
            if attempt["worker_id"] == "m1":
                until(f"{task} running on m1", lambda: self.belt(
                    "show", task, "--limit", "64")["attempts"]["items"][0]["state"] == "active", 60)
                return task, token, batch
        raise RuntimeError(f"none of {limit} host submissions was placed on m1")

    def finished_once_on_m1(self, task, token, batch):
        views = self.finish(batch)
        worker = views[task]["attempts"]["items"][0]["worker_id"]
        if worker != "m1":
            raise RuntimeError(f"{task} ran on {worker}, not m1")
        counts = self.starts(token)
        if counts != {"m1": 1}:
            raise RuntimeError(f"{task}: the command started {counts}, expected once on m1")
        return views

    def m1(self):
        n = BASE / LINUX_BINARY
        # The spec's order for a host that becomes a member: an existing
        # member installs it as a worker, then `init` runs there.
        self.install("m1")
        name = self.m1_container()
        command(["docker", "exec", name, "install", "-d", "-o", "belt", "-g", "belt", "-m", "700",
                 "/home/belt/project", "/home/belt/s26-bin"])
        command(["docker", "cp", self.project / "job.sh", f"{name}:/home/belt/project/job.sh"])
        command(["docker", "exec", name, "chown", "belt:belt", "/home/belt/project/job.sh"])
        ssh("m1", 'cd "$HOME/project" && git init -q && git add job.sh && '
            'git -c user.name="S26 Harness" -c user.email=s26@example.invalid '
            '-c commit.gpgsign=false commit -qm "S26 fixture"')
        self.count += 1
        p = ssh("m1", USER_BUS + 'exec "$HOME/.config/belt/bin/current/belt" --json init',
                check=False, timeout=180)
        stem = RUN / "evidence" / f"{self.count:04d}-m1-init"
        stem.with_suffix(".stdout.json").write_text(p.stdout)
        stem.with_suffix(".stderr.txt").write_text(p.stderr)
        if p.returncode:
            raise RuntimeError(f"belt init on m1 exited {p.returncode}: {(p.stderr + p.stdout)[-1800:]}")
        self.service("m1", "active", kind="local")
        p = ssh("m1", 'test "$(id -u)" = 1000 && test "$(cat /proc/1/comm)" = systemd && '
                'test "$(loginctl show-user 1000 -p Linger --value)" = yes', check=False)
        if p.returncode:
            raise RuntimeError("m1: wrong user, PID 1, or lingering")
        # One resident per member: no worker unit may still be active.
        units = ssh("m1", USER_BUS + 'for u in "$HOME"/.config/systemd/user/io.github.t09tanaka.belt.worker.*.service; do '
                    '[ -e "$u" ] && systemctl --user is-active "$(basename "$u")"; done; true').stdout.split()
        if "active" in units:
            raise RuntimeError("m1 still runs its worker-only unit next to the init resident")
        if self.m1_link() != sha256(n):
            raise RuntimeError("m1 bin/resident does not name the installed binary's digest")
        if self.m1_main_exe() != f"/home/belt/.config/belt/bin/{sha256(n)}/belt":
            raise RuntimeError(f"m1 resident runs {self.m1_main_exe()}, not the installed version")
        dbs = ssh("m1", 'find "$HOME" -name worker.db -not -path "*/upgrade/*"').stdout.split()
        if len(dbs) != 1:
            raise RuntimeError(f"expected one worker database on m1, found {dbs}")
        self.m1_state = os.path.dirname(dbs[0])
        cluster = json.loads(self.input.read_text())["expected_cluster"]
        if cluster not in json.dumps(self.m1_belt("group")):
            raise RuntimeError("m1 does not report the host's group")
        task, token, batch = self.place_on_m1(15)
        save(RUN / "evidence/m1.json", self.finished_once_on_m1(task, token, batch))
        self.m1_ready = True
        return ("m1 installed then initialized: one active init resident (user service, "
                "lingering) on the host's group; a host submission ran there once")

    # A57: `belt upgrade` on m1 (§24.8). These need a Linux build with the
    # upgrade subcommand and the variants from `build-linux.sh --upgrade`.

    VARIANTS = {"n": LINUX_BINARY, "n1": LINUX_BINARY + "-n1",
                "broken": LINUX_BINARY + "-n1-broken"}

    def a57_ready(self, *need):
        if not self.m1_ready:
            raise Skip("m1 is not an initialized member (M1 did not pass)")
        p = ssh("m1", USER_BUS + '"$HOME/.config/belt/bin/resident/belt" upgrade --help', check=False)
        if p.returncode:
            raise Skip("the belt on m1 has no `upgrade` subcommand; build a branch that has it "
                       "with build-linux.sh --upgrade <ref> (README)")
        files = {k: BASE / v for k, v in self.VARIANTS.items()}
        missing = [files[k].name for k in need if not files[k].is_file()]
        if missing:
            raise Skip(f"run build-linux.sh --upgrade <ref> first; missing {missing}")
        digests = {k: sha256(f) for k, f in files.items() if f.is_file()}
        if len(set(digests.values())) != len(digests):
            raise RuntimeError(f"the variant binaries are not distinct versions: {digests}")
        if not getattr(self, "copied", False):
            name = self.m1_container()
            for k in digests:
                command(["docker", "cp", files[k], f"{name}:/home/belt/s26-bin/belt-{k}"])
                command(["docker", "exec", name, "sh", "-ec",
                         f"chown belt:belt /home/belt/s26-bin/belt-{k}; "
                         f"chmod 755 /home/belt/s26-bin/belt-{k}"])
            self.copied = True
        return digests

    def other(self, digests):
        # Each upgrade needs a version other than the one m1 runs.
        current = self.m1_link()
        if current not in (digests["n"], digests["n1"]):
            raise RuntimeError(f"m1 runs neither N nor N+1 ({current})")
        return current, "n1" if current == digests["n"] else "n"

    def m1_upgrade(self, key, direct=False):
        """Start an upgrade on m1 in the background. The shell writes its own
        pid, then execs belt, so the pid file names the upgrade CLI. With
        `direct` the target binary runs `upgrade` itself, which is what
        `--binary` delegates to; then the pid file names the process that
        holds the trial."""
        self.count += 1
        stem = RUN / "evidence" / f"{self.count:04d}-m1-upgrade-{key}"
        binary = f'"$HOME/s26-bin/belt-{key}"'
        cli = (f"{binary} --json upgrade" if direct else
               f'"$HOME/.config/belt/bin/resident/belt" --json upgrade --binary {binary}')
        script = (USER_BUS + 'echo $$ > "$HOME/s26-bin/upgrade.pid"; exec ' + cli)
        out = open(stem.with_suffix(".stdout.json"), "w")
        err = open(stem.with_suffix(".stderr.txt"), "w")
        proc = subprocess.Popen([str(RUN / "bin/ssh"), "-T", "--", "m1", script],
                                stdin=subprocess.DEVNULL, stdout=out, stderr=err)
        out.close()
        err.close()
        return proc, stem

    @staticmethod
    def ended(proc, stem, seconds):
        try:
            code = proc.wait(timeout=seconds)
        except subprocess.TimeoutExpired:
            proc.terminate()  # our own ssh client, by its Popen handle
            proc.wait(timeout=15)
            raise RuntimeError(f"upgrade on m1 did not end within {seconds}s; see {stem}*")
        text = stem.with_suffix(".stdout.json").read_text()
        return code, (text + stem.with_suffix(".stderr.txt").read_text())[-1500:]

    # Waits inside m1 until the trial marker exists and the resident's main
    # process runs the target version, then signals the upgrade CLI named by
    # the pid file: STOP holds the trial open, TERM ends the CLI so that only
    # the guard can decide. The process is identified by its executable and
    # argv first; with --binary the CLI is the one child of the named pid.
    WATCH = r'''set -eu
marker="$1/upgrade-trial.json"; want="$HOME/.config/belt/bin/$2/belt"
sig=$3; exe=$4; child=$5; deadline=$((SECONDS + $6))
set -- "$HOME"/.config/systemd/user/io.github.t09tanaka.belt.local.*.service
unit=$(basename "$1")
while :; do
  if [ -e "$marker" ]; then
    main=$(systemctl --user -M belt@ show -p MainPID --value "$unit" 2>/dev/null || echo 0)
    if [ "${main:-0}" -gt 1 ] && [ "$(readlink "/proc/$main/exe" 2>/dev/null)" = "$want" ]; then break; fi
  fi
  [ "$SECONDS" -lt "$deadline" ] || { echo "no trial of $want seen" >&2; exit 3; }
  sleep 0.02
done
pid=$(cat "$HOME/s26-bin/upgrade.pid")
if [ "$child" = yes ]; then pid=$(pgrep -P "$pid"); fi
case "$pid" in ''|*[!0-9]*) echo "no single upgrade process: '$pid'" >&2; exit 4 ;; esac
[ "$pid" -gt 1 ] || exit 4
[ "$(readlink "/proc/$pid/exe")" = "$(readlink -f "$exe")" ] || { echo "pid $pid is not $exe" >&2; exit 4; }
tr '\0' ' ' < "/proc/$pid/cmdline" | grep -q ' upgrade' || { echo "pid $pid is not an upgrade" >&2; exit 4; }
kill -s "$sig" "$pid"
echo "$pid"
'''

    SIGNAL = r'''set -eu
pid=$1; exe=$2
case "$pid" in ''|*[!0-9]*) exit 4 ;; esac
[ "$pid" -gt 1 ] || exit 4
[ "$(readlink "/proc/$pid/exe")" = "$(readlink -f "$exe")" ] || exit 4
tr '\0' ' ' < "/proc/$pid/cmdline" | grep -q ' upgrade' || exit 4
kill -s CONT "$pid"
'''

    def m1_watch(self, digest, sig, exe, child, seconds):
        return subprocess.Popen(
            self.m1_root(self.WATCH, self.m1_state, digest, sig, exe,
                         "yes" if child else "no", str(seconds)),
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    @staticmethod
    def watched(watcher, seconds):
        try:
            out, err = watcher.communicate(timeout=seconds)
        except subprocess.TimeoutExpired:
            watcher.terminate()  # our own docker client, by its Popen handle
            watcher.communicate(timeout=15)
            raise RuntimeError("the trial watcher on m1 did not finish")
        if watcher.returncode:
            raise RuntimeError(f"trial watcher on m1: {err.strip() or watcher.returncode}")
        return out.strip()

    def a57_rollback(self):
        digests = self.a57_ready("n", "broken")
        if self.m1_link() != digests["n"]:
            raise RuntimeError("m1 must run N before the rollback check")
        before = self.m1_version_file(digests["n"])
        task, token, batch = self.place_on_m1(20)
        broken = "/home/belt/s26-bin/belt-broken"
        # The broken build's resident never answers health. The CLI is ended
        # once the broken version runs, so only the guard can restore.
        watcher = self.m1_watch(digests["broken"], "TERM", broken, True, 240)
        proc, stem = self.m1_upgrade("broken")
        self.watched(watcher, 260)
        ended = time.monotonic()
        code, tail = self.ended(proc, stem, 60)
        if code == 0:
            raise RuntimeError(f"the upgrade to the broken build reported success: {tail}")
        until("guard restored N", lambda: self.m1_upgraded_away()
              and self.m1_link() == digests["n"], 90)
        took = time.monotonic() - ended
        self.service("m1", "active", kind="local")
        until("m1 resident runs N again", lambda: self.m1_main_exe()
              == f"/home/belt/.config/belt/bin/{digests['n']}/belt", 45)
        if self.m1_version_file(digests["n"]) != before:
            raise RuntimeError("the N version directory changed")
        self.m1_belt("ls")
        save(RUN / "evidence/a57-rollback.json", self.finished_once_on_m1(task, token, batch))
        return (f"broken N+1 not committed; with its CLI ended, the guard restored N "
                f"{took:.0f}s later (deadline 45s from the start); N's directory untouched; "
                "the host run on m1 finished, started once")

    def a57_commit(self):
        digests = self.a57_ready("n", "n1")
        current, key = self.other(digests)
        before = self.m1_version_file(current)
        task, token, batch = self.place_on_m1(20)
        proc, stem = self.m1_upgrade(key)
        code, tail = self.ended(proc, stem, 300)
        if code:
            raise RuntimeError(f"belt upgrade --binary {key} exited {code}: {tail}")
        if self.m1_link() != digests[key]:
            raise RuntimeError(f"bin/resident is {self.m1_link()}, not {key}")
        if self.m1_version_file(current) != before:
            raise RuntimeError("the previous version directory changed")
        if self.m1_main_exe() != f"/home/belt/.config/belt/bin/{digests[key]}/belt":
            raise RuntimeError("m1 resident does not run the new version")
        if not self.m1_upgraded_away():
            raise RuntimeError("an upgrade record remains after the commit")
        save(RUN / "evidence/a57-commit.json", self.finished_once_on_m1(task, token, batch))
        return (f"committed {current[:12]} -> {digests[key][:12]} while a host run was on m1; "
                "bin/resident switched, old directory untouched, the run finished, started once")

    def a57_trial_refusal(self):
        digests = self.a57_ready("n", "n1")
        current, key = self.other(digests)
        # The upgrade waits until m1's worker has no live attempt before it
        # switches, and the hold keeps new ones off until the commit, so no
        # attempt of the host's is on m1 during the trial and a cancel from
        # the host has nothing there to reach. The cancel the trial refuses
        # is m1's own, of its run: it requires macOS, so it stays unplaced.
        own = self.m1_belt("submit", "--requires", "macos", "--", "sh", "job.sh", "5")["task_id"]
        target = f"/home/belt/s26-bin/belt-{key}"
        watcher = self.m1_watch(digests[key], "STOP", target, False, 240)
        proc, stem = self.m1_upgrade(key, direct=True)
        pid, answer = None, None
        try:
            pid = self.watched(watcher, 260)
            held = time.monotonic()
            if self.m1_upgraded_away():
                raise RuntimeError("missed the trial window: no trial was held open (incomplete)")
            while time.monotonic() - held < 8 and answer is None:
                p = self.m1_belt("cancel", own, check=False, timeout=20)
                try:
                    value = json.loads(p.stdout)
                except ValueError:
                    time.sleep(0.3)  # the new version is not answering yet
                    continue
                if value.get("ok") is not False:
                    raise RuntimeError(f"the version on trial accepted a cancel: {value}")
                error = value.get("error") or {}
                if error.get("code") == "OVERLOADED":
                    answer = error.get("message", "")
                else:
                    time.sleep(0.3)
            if answer is None:
                raise RuntimeError("the version on trial gave no OVERLOADED answer within 8s")
            held_for = time.monotonic() - held
            if self.m1_belt("show", own)["task"]["cancel_requested_at_ms"] is not None:
                raise RuntimeError("the refused cancel was saved")
        finally:
            if pid:
                self.m1_exec(self.SIGNAL, pid, target)
        code, tail = self.ended(proc, stem, 120)
        if code:
            raise RuntimeError(f"upgrade after the held trial exited {code}: {tail}")
        if self.m1_link() != digests[key]:
            raise RuntimeError("the held upgrade did not commit")
        # The run was never placed, so the cancel settles it at once.
        result = self.m1_belt("cancel", own)
        task = self.m1_belt("show", own)
        if not (result.get("cancel_requested") and result.get("stop_confirmed")
                and task["task"]["cancel_requested_at_ms"] is not None
                and not task["attempts"]["items"]):
            raise RuntimeError(f"the cancel after the commit did not take effect: {result} {task}")
        return (f"during a trial held open {held_for:.0f}s m1 refused its own cancel with "
                f"OVERLOADED ({answer!r}) and saved nothing; "
                "after the commit the same cancel took effect. A host cancel of a run on m1 "
                "cannot arise during a trial (the upgrade drains m1 first)")

    def a57_slow_drain(self):
        digests = self.a57_ready("n", "n1")
        current, key = self.other(digests)
        task, token, batch = self.place_on_m1(75)
        started = time.monotonic()
        proc, stem = self.m1_upgrade(key)
        code, tail = self.ended(proc, stem, 400)
        took = time.monotonic() - started
        if code:
            raise RuntimeError(f"after a drain of {took:.0f}s (> the 45s deadline) the upgrade "
                               f"exited {code}, not committed: {tail}")
        if self.m1_link() != digests[key] or not self.m1_upgraded_away():
            raise RuntimeError("the slow-drain upgrade did not stay committed")
        save(RUN / "evidence/a57-slow-drain.json", self.finished_once_on_m1(task, token, batch))
        return (f"upgrade waited {took:.0f}s for a 75s host run on m1, then committed "
                f"{digests[key][:12]}; the run started once")

    def check(self, label, fn):
        try:
            detail, status = fn(), "PASS"
        except Skip as skip:
            detail, status = str(skip), "SKIP"
        except Exception as error:
            detail, status = str(error), "FAIL"
        self.results.append({"check": label, "status": status, "pass": status == "PASS",
                             "reason": detail})
        print(f"{status} {label}: {detail}", flush=True)

    def close(self):
        # Only our Popen handle is signalled; no PID files or broad process matching.
        if self.parent and self.parent.poll() is None:
            for task in self.tasks:
                try:
                    self.belt("cancel", task, timeout=15)
                except Exception:
                    pass
            self.parent.terminate()
            try:
                self.parent.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.parent.kill()
                self.parent.wait()


def run():
    checks = Checks()
    try:
        checks.start()
        checks.check("A02", checks.a02)
        checks.check("A45 (part)", checks.a45)
        checks.check("A01", checks.a01)
        checks.check("A34 (part)", checks.a34)
        checks.check("M1 init member", checks.m1)
        # A57 needs `belt upgrade` (R68); each check skips, with its reason,
        # when the Linux belt on m1 has no such subcommand.
        checks.check("A57 rollback", checks.a57_rollback)
        checks.check("A57 commit", checks.a57_commit)
        checks.check("A57 trial refusal", checks.a57_trial_refusal)
        checks.check("A57 slow drain", checks.a57_slow_drain)
    finally:
        checks.close()
        save(RUN / "evidence/summary.json", checks.results)
    count = {k: sum(r["status"] == k for r in checks.results) for k in ("PASS", "SKIP", "FAIL")}
    print(f"Summary: {count['PASS']} PASS, {count['SKIP']} SKIP, {count['FAIL']} FAIL "
          f"of {len(checks.results)}; evidence: {RUN / 'evidence'}")
    return 1 if count["FAIL"] else 0


def variants():
    """Export one Git ref three times for build-linux.sh --upgrade: N as it
    is, N+1 with the next patch version, and a broken N+1 whose resident never
    answers health. The patches exist only in these exports; nothing in the
    product source is changed or committed."""
    ref = sys.argv[2]
    out = BASE / "linux-cache/src"
    for q in [out, *out.parents]:
        if q.is_symlink():
            raise RuntimeError(f"refusing symlink: {q}")
    if out.exists():
        shutil.rmtree(out)
    commit = command(["git", "-C", ROOT, "rev-parse", "--verify", ref + "^{commit}"]).stdout.strip()
    for name in ("n", "n1", "n1-broken"):
        (out / name).mkdir(parents=True)
        archive = subprocess.run(["git", "-C", str(ROOT), "archive", "--format=tar", commit],
                                 stdout=subprocess.PIPE, check=True).stdout
        # tar would keep the commit time; Cargo compares mtimes with its last
        # build, so an export of an older commit could look already built.
        subprocess.run(["tar", "-x", "-m", "-C", str(out / name)], input=archive, check=True)

    def patch(path, old, new):
        text = path.read_text()
        if text.count(old) != 1:
            raise RuntimeError(f"{path.name}: expected one {old!r}, found {text.count(old)}")
        path.write_text(text.replace(old, new))

    manifest = (out / "n/Cargo.toml").read_text()
    package = manifest.split("[package]", 1)[1].split("\n[", 1)[0]
    (version,) = [line.split('"')[1] for line in package.splitlines()
                  if line.startswith("version = ")]
    major, minor, micro = version.split(".")
    next_version = f"{major}.{minor}.{int(micro) + 1}"
    for name in ("n1", "n1-broken"):
        patch(out / name / "Cargo.toml", f'version = "{version}"', f'version = "{next_version}"')
        patch(out / name / "Cargo.lock", f'name = "belt-cli"\nversion = "{version}"',
              f'name = "belt-cli"\nversion = "{next_version}"')
    # The resident (`belt daemon`) of the broken build starts and then never
    # answers: the upgrade's health check fails while the process runs.
    patch(out / "n1-broken/src/cli.rs",
          "    if let Command::Daemon(args) = &cli.command {\n",
          "    if let Command::Daemon(args) = &cli.command {\n"
          "        // S26 harness export only: a resident that never becomes healthy.\n"
          "        if std::env::var_os(\"S26_NEVER_SET\").is_none() {\n"
          "            loop {\n"
          "                std::thread::sleep(std::time::Duration::from_secs(3600));\n"
          "            }\n"
          "        }\n")
    print(f"exported {commit[:12]} as N {version}, N+1 {next_version} and broken N+1")


def down():
    if not RUN.exists():
        print("No run directory; nothing to remove.")
        return
    manifest = json.loads((RUN / "resources.json").read_text())
    token = manifest["token"]
    # Filter by the unguessable per-up label, never by broad name or image.
    for kind, listing, removal in (
        ("container", ["docker", "ps", "-aq"], ["docker", "rm", "-f", "-v"]),
        ("network", ["docker", "network", "ls", "-q"], ["docker", "network", "rm"]),
    ):
        ids = command([*listing, "--filter", f"label={LABEL}={token}"]).stdout.split()
        for ident in ids:
            data = json.loads(command(["docker", kind, "inspect", ident]).stdout)[0]
            labels = data.get("Config", {}).get("Labels", {}) if kind == "container" else data["Labels"]
            if labels.get(LABEL) != token:
                raise RuntimeError(f"refusing foreign {kind}: {ident}")
            command([*removal, ident])
    shutil.rmtree(RUN)
    print("Removed this run's containers, network and private run directory; caches retained.")


def interrupted(signum, frame):
    raise KeyboardInterrupt(f"signal {signum}")


if __name__ == "__main__":
    os.umask(0o077)
    signal.signal(signal.SIGTERM, interrupted)
    BASE.mkdir(parents=True, exist_ok=True)
    # up/run/down cannot race, and down refuses while the foreground runner lives.
    with open(BASE / "harness.lock", "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            action = {"up": up, "run": run, "down": down, "variants": variants}[sys.argv[1]]
            sys.exit(action() or 0)
        except (Exception, KeyboardInterrupt) as error:
            print(f"FAIL harness: {error}", file=sys.stderr)
            print("Summary: harness incomplete; no acceptance claim.", file=sys.stderr)
            sys.exit(1)
