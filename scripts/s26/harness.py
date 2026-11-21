#!/usr/bin/env python3
"""Docker/SSH acceptance driver; all belt calls use a private environment."""
import fcntl
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
RUN = Path(os.environ["S26_RUN"])
LABEL = "io.belt.s26.run"
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


def ssh(host, script, check=True):
    # Always use our -F wrapper, never the caller's SSH configuration.
    return command([RUN / "bin/ssh", "-T", "--", host, script],
                   timeout=30, check=check)


def up():
    RUN.mkdir(mode=0o700)
    (RUN / "bin").mkdir()
    token = secrets.token_hex(16)
    prefix = "s26-" + os.environ["PORTOOL_WORKTREE_ID"][:12] + "-" + token[:8]
    manifest = {"token": token, "network": prefix,
                "containers": [prefix + "-w1", prefix + "-w2"]}
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
    for host in ("w1", "w2"):
        config += [f"Host {host}", f"  Port {os.environ['S26_' + host.upper() + '_PORT']}"]
    (RUN / "ssh_config").write_text("\n".join(config) + "\n")
    wrapper = RUN / "bin/ssh"
    wrapper.write_text("#!/usr/bin/env bash\nset -euo pipefail\nexec /usr/bin/ssh -F "
                       + shlex.quote(str(RUN / "ssh_config")) + ' "$@"\n')
    wrapper.chmod(0o700)
    command(["docker", "network", "create", "--label", f"{LABEL}={token}", prefix])
    for host, name in zip(("w1", "w2"), manifest["containers"]):
        port = os.environ["S26_" + host.upper() + "_PORT"]
        command(["docker", "run", "-d", "--platform", "linux/arm64",
                 "--privileged", "--cgroupns=host", "--tmpfs", "/run",
                 "--tmpfs", "/run/lock", "--tmpfs", "/tmp",
                 "-v", "/sys/fs/cgroup:/sys/fs/cgroup:rw",
                 "--label", f"{LABEL}={token}", "--name", name,
                 "--hostname", host, "--network", prefix,
                 "-p", f"127.0.0.1:{port}:22",
                 "belt-s26-worker:debian12.9-fw0.4.0"])
        command(["docker", "cp", RUN / "id_ed25519.pub",
                 f"{name}:/home/belt/.ssh/authorized_keys"])
        command(["docker", "exec", name, "sh", "-ec",
                 "chown belt:belt /home/belt/.ssh/authorized_keys; "
                 "chmod 600 /home/belt/.ssh/authorized_keys"])
        until(f"{host}: SSH and lingering user manager", lambda: ssh(
            host, 'test "$(cat /proc/1/comm)" = systemd && '
            'test "$(loginctl show-user 1000 -p Linger --value)" = yes && '
            'test -S /run/user/1000/bus', check=False).returncode == 0)
    print(f"Workers ready. Evidence and private SSH files: {RUN}")


class Checks:
    def __init__(self):
        binary = Path(os.environ["S26_BELT"])
        if not binary.is_absolute() or not binary.is_file():
            raise RuntimeError("S26_BELT must name an existing absolute native binary")
        if not (BASE / "belt-linux-aarch64").is_file():
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
            '#!/bin/sh\nset -eu\nprintf "s26 start %s\\n" "$(hostname)"\n'
            'sleep "$1"\nprintf "s26 complete %s\\n" "$(hostname)"\n')
        command(["git", "add", "job.sh"], env=self.env, cwd=self.project)
        command(["git", "-c", "commit.gpgsign=false", "commit", "-qm", "S26 fixture"],
                env=self.env, cwd=self.project)
        self.count = 0
        self.tasks = []
        self.parent = None
        self.results = []

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
                  str(BASE / "belt-linux-aarch64"), timeout=180)
        self.service(host, "active")

    def service(self, host, expected):
        script = ('export XDG_RUNTIME_DIR=/run/user/1000; '
                  'export DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus; '
                  'set -- "$HOME"/.config/systemd/user/io.github.t09tanaka.belt.worker.*.service; '
                  '[ "$#" -eq 1 ] && [ -f "$1" ] || exit 90; '
                  'systemctl --user is-active "$(basename "$1")"')

        def state():
            p = ssh(host, script, check=False)
            # Inactive is code 3; transport/missing-unit errors are never a pass.
            return p.stdout.strip() == expected and p.returncode == (0 if expected == "active" else 3)
        until(f"{host} user service {expected}", state, 45)

    def submit(self, seconds, count):
        tasks = []
        for _ in range(count):
            receipt = self.belt("submit", "--", "sh", "job.sh", str(seconds))
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

    def check(self, label, fn):
        try:
            detail = fn()
            passed = True
        except Exception as error:
            detail, passed = str(error), False
        self.results.append({"check": label, "pass": passed, "reason": detail})
        print(f"{'PASS' if passed else 'FAIL'} {label}: {detail}", flush=True)

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
    finally:
        checks.close()
        save(RUN / "evidence/summary.json", checks.results)
    passed = sum(r["pass"] for r in checks.results)
    print(f"Summary: {passed}/4 PASS; evidence: {RUN / 'evidence'}")
    return 0 if passed == 4 else 1


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
            action = {"up": up, "run": run, "down": down}[sys.argv[1]]
            sys.exit(action() or 0)
        except (Exception, KeyboardInterrupt) as error:
            print(f"FAIL harness: {error}", file=sys.stderr)
            print("Summary: harness incomplete; no acceptance claim.", file=sys.stderr)
            sys.exit(1)
