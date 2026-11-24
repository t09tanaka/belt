"""S26 --linux-parent: a Linux container is the parent (an `init` member),
another Linux container is its worker. Everything the parent does goes through
its own installed binary over SSH, the way a user at that machine would.

Container roles (the names come from `up.sh`): m1 is the parent, w1 its worker.
w2 stays idle. The parent reaches w1 over a test-only key kept inside the
containers; the host's ~/.ssh is never touched.
"""
import json
import secrets
import shlex

from harness import (BASE, LINUX_BINARY, RUN, USER_BUS, Skip, command, save,
                     sha256, ssh, until)
from peers import controlled_service

PARENT = "m1"
WORKER = "w1"
RESIDENT = '"$HOME/.config/belt/bin/resident/belt"'
HOLD = ('set -eu; mkdir -p "$HOME/s26-starts"; hostname >> "$HOME/s26-starts/$1"; '
        'echo "s26 hold $1 on $(hostname)"; '
        'for i in $(seq 1 420); do test ! -e "$HOME/s26-release-$1" || { echo "s26 released $1"; exit 0; }; '
        'sleep 1; done; exit 1')


class Parent:
    def __init__(self):
        if not (BASE / LINUX_BINARY).is_file():
            raise RuntimeError("run build-linux.sh first")
        self.count = 0
        self.results = []
        self.tasks = []
        manifest = json.loads((RUN / "resources.json").read_text())
        self.container = [c for c in manifest["containers"] if c.endswith("-" + PARENT)][0]
        self.worker_container = [c for c in manifest["containers"] if c.endswith("-" + WORKER)][0]
        (RUN / "evidence").mkdir(exist_ok=True)

    # -- belt on the parent -------------------------------------------------
    def belt(self, *args, timeout=120, check=True, binary=None, text=False):
        self.count += 1
        script = (USER_BUS + 'cd "$HOME/project" && exec ' + (binary or RESIDENT)
                  + ('' if text else ' --json') + ' ' + " ".join(shlex.quote(str(a)) for a in args))
        p = ssh(PARENT, script, check=False, timeout=timeout)
        stem = RUN / "evidence" / f"{self.count:04d}-lp-{args[0]}"
        stem.with_suffix(".stdout.json").write_text(p.stdout)
        stem.with_suffix(".stderr.txt").write_text(p.stderr)
        if not check:
            return p
        if text:
            if p.returncode:
                raise RuntimeError(f"parent belt {args[0]} exited {p.returncode}: {(p.stderr + p.stdout)[-800:]}")
            return p.stdout + p.stderr
        if p.returncode:
            raise RuntimeError(f"parent belt {' '.join(map(str, args))} exited {p.returncode}; "
                               f"see {stem}*: " + (p.stderr + p.stdout)[-1800:])
        value = json.loads(p.stdout)
        if value.get("ok") is False:
            raise RuntimeError(f"parent belt returned an error: {value}")
        return value.get("result", value.get("data", value))

    def starts(self, token):
        counts = {}
        for host in (PARENT, WORKER):
            lines = ssh(host, f'cat "$HOME/s26-starts/{token}" 2>/dev/null || true').stdout.splitlines()
            if lines:
                counts[host] = len(lines)
        return counts

    def attempts(self, task):
        return self.belt("show", task, "--limit", "64")["attempts"]["items"]

    # -- setup --------------------------------------------------------------
    def setup(self):
        src = BASE / LINUX_BINARY
        digest = sha256(src)
        self.digest = digest
        for name in (self.container, self.worker_container):
            command(["docker", "exec", name, "install", "-d", "-o", "belt", "-g", "belt",
                     "-m", "700", "/home/belt/s26-bin"])
        command(["docker", "cp", src, f"{self.container}:/home/belt/s26-bin/belt"])
        command(["docker", "exec", self.container, "sh", "-ec",
                 "chown belt:belt /home/belt/s26-bin/belt; chmod 755 /home/belt/s26-bin/belt; "
                 "install -d -o belt -g belt -m 700 /home/belt/project"])
        ssh(PARENT, 'cd "$HOME/project" && git init -q && '
            'printf "fixture\\n" > README && git add README && '
            'git -c user.name="S26 Harness" -c user.email=s26@example.invalid '
            '-c commit.gpgsign=false commit -qm "S26 fixture"')
        # Test-only key inside the containers: the parent reaches the worker by
        # container name on the harness network. The host's ~/.ssh is not used.
        pub = ssh(PARENT, 'mkdir -p "$HOME/.ssh" && chmod 700 "$HOME/.ssh" && '
                  'ssh-keygen -q -t ed25519 -N "" -f "$HOME/.ssh/s26-w1" && '
                  'cat "$HOME/.ssh/s26-w1.pub"').stdout.strip()
        ssh(WORKER, "printf '%s\\n' " + shlex.quote(pub) + ' >> "$HOME/.ssh/authorized_keys"')
        host_key = ssh(WORKER, "cat /etc/ssh/ssh_host_ed25519_key.pub").stdout.strip()
        config = (f"Host w1\n  HostName {self.worker_container}\n  User belt\n  HostKeyAlias w1\n"
                  "  IdentityFile ~/.ssh/s26-w1\n  IdentitiesOnly yes\n  BatchMode yes\n"
                  "  StrictHostKeyChecking yes\n  ConnectTimeout 5\n")
        ssh(PARENT, "umask 077; printf %s " + shlex.quote(config) + ' > "$HOME/.ssh/config"; '
            + "printf '%s\\n' " + shlex.quote("w1 " + host_key) + ' >> "$HOME/.ssh/known_hosts"; '
            + "ssh w1 true")

    def init(self):
        direct = '"$HOME/s26-bin/belt"'
        p = self.belt("init", "--name", PARENT, check=False, binary=direct, timeout=180)
        if p.returncode:
            raise RuntimeError(f"belt init exited {p.returncode}: {(p.stderr + p.stdout)[-1800:]}")
        self.service("active")
        facts = ssh(PARENT, 'test "$(id -u)" = 1000 && test "$(cat /proc/1/comm)" = systemd && '
                    'test "$(loginctl show-user 1000 -p Linger --value)" = yes', check=False)
        if facts.returncode:
            raise RuntimeError("parent: wrong user, PID 1, or lingering")
        link = ssh(PARENT, 'readlink "$HOME/.config/belt/bin/resident"').stdout.strip()
        if link != self.digest:
            raise RuntimeError(f"bin/resident is {link}, not the installed digest {self.digest}")
        units = ssh(PARENT, USER_BUS + 'systemctl --user list-units --state=active --plain --no-legend '
                    "'io.github.t09tanaka.belt.*'").stdout.split("\n")
        local = [u for u in units if ".belt.local." in u]
        if len(local) != 1:
            raise RuntimeError(f"expected one active init unit, saw {units}")
        group = self.belt("group")
        if not str(group.get("group_id", "")).startswith("cl_"):
            raise RuntimeError(f"belt group names no cluster: {group}")
        return "belt init on Linux registered one lingering systemd user resident running the installed digest"

    def service(self, expected):
        script = (USER_BUS + 'set -- "$HOME"/.config/systemd/user/io.github.t09tanaka.belt.local.*.service; '
                  '[ "$#" -eq 1 ] && [ -f "$1" ] || exit 90; '
                  'systemctl --user is-active "$(basename "$1")"')

        def state():
            p = ssh(PARENT, script, check=False)
            return p.stdout.strip() == expected and p.returncode == (0 if expected == "active" else 3)
        until(f"parent resident {expected}", state, 45)

    def add_worker(self):
        self.belt("workers", "add", WORKER, "--ssh", WORKER, "--install", "--binary",
                  "/home/belt/s26-bin/belt", timeout=180)
        until("worker connected", lambda: any(
            w["id"] == WORKER and w["connection"] == "connected"
            for w in self.belt("workers")["workers"]), 90)
        return "parent installed and connected its worker over container SSH"

    # -- scenarios ----------------------------------------------------------
    def place_on_worker(self, script=HOLD, limit=5):
        """Submit blocking jobs until one is placed on the worker; the others
        (taken by the parent's own worker) are released and awaited."""
        local = []
        try:
            for _ in range(limit):
                token = secrets.token_hex(8)
                task = self.belt("submit", "--", "sh", "-c", script, "s26", token)["task_id"]
                self.tasks.append(task)
                attempt = until(f"{task} placed", lambda: (self.attempts(task) or [None])[0], 60)
                if attempt["worker_id"] == WORKER:
                    until("command running on worker", lambda: self.starts(token) == {WORKER: 1}, 60)
                    return task, token, attempt["attempt_id"]
                local.append((task, token))
            raise RuntimeError(f"none of {limit} submissions reached {WORKER}")
        finally:
            for task, token in local:
                ssh(PARENT, f'touch "$HOME/s26-release-{token}"')
                self.belt("wait", task, timeout=90)

    def release(self, token):
        ssh(WORKER, f'touch "$HOME/s26-release-{token}"')

    def run_on_worker(self):
        task, token, attempt_id = self.place_on_worker()
        self.release(token)
        self.belt("wait", task, timeout=120)
        (attempt,) = self.attempts(task)
        if (attempt["attempt_id"] != attempt_id or attempt["worker_id"] != WORKER
                or attempt["collection_verdict"] != "complete"
                or (attempt["termination"] or {}).get("outcome") != "succeeded"):
            raise RuntimeError(f"result not collected from the worker: {attempt}")
        if self.starts(token) != {WORKER: 1}:
            raise RuntimeError(f"command started {self.starts(token)}, expected once on {WORKER}")
        text = self.belt("logs", task, text=True)
        if f"s26 hold {token} on {WORKER}" not in text or f"s26 released {token}" not in text:
            raise RuntimeError(f"collected logs lack the worker's output: {text[:600]}")
        save(RUN / "evidence/lp-run-on-worker.json", {"task": task, "attempt": attempt})
        return "Linux parent placed a run on the worker container; result collected, command started once, logs readable"

    def exec_on_worker(self):
        """`belt exec` from the parent, pushed onto the worker by keeping the
        parent's own worker busy with a held job."""
        busy = []
        try:
            held = []
            for _ in range(4):
                token = secrets.token_hex(8)
                task = self.belt("submit", "--", "sh", "-c", HOLD, "s26", token)["task_id"]
                self.tasks.append(task)
                held.append((task, token))
                until(f"{task} placed", lambda: (self.attempts(task) or [None])[0], 60)
                places = {self.attempts(t)[0]["worker_id"] for t, _ in held}
                if places >= {PARENT, WORKER}:
                    break
            busy = held
            if {self.attempts(t)[0]["worker_id"] for t, _ in held} < {PARENT, WORKER}:
                raise RuntimeError("could not occupy both workers")
            # Both slots are full; free the worker only, so exec has to use it.
            on_worker = [(t, k) for t, k in held if self.attempts(t)[0]["worker_id"] == WORKER]
            for _, k in on_worker:
                self.release(k)
            for t, _ in on_worker:
                self.belt("wait", t, timeout=90)
            out = self.belt("exec", "--", "sh", "-c", 'echo "s26 exec on $(hostname)"',
                            text=True, timeout=180)
            task = next((w for w in out.split() if w.startswith("task_")), None)
            if not task:
                raise RuntimeError(f"exec printed no task id: {out[-400:]}")
            (attempt,) = self.attempts(task)
            logs = self.belt("logs", task, text=True)
            if attempt["worker_id"] != WORKER or f"s26 exec on {WORKER}" not in logs:
                raise RuntimeError(f"exec ran on {attempt['worker_id']}; logs: {logs[-400:]}")
        finally:
            for t, k in busy:
                ssh(PARENT, f'touch "$HOME/s26-release-{k}"')
                ssh(WORKER, f'touch "$HOME/s26-release-{k}"')
                self.belt("wait", t, timeout=90, check=False)
        return "belt exec on the Linux parent ran on the worker and returned its output"

    def cancel(self):
        task, token, attempt_id = self.place_on_worker()
        self.belt("cancel", task)

        def ended():
            (a,) = self.attempts(task)
            return a if (a["termination"] or {}).get("outcome") else None
        attempt = until("cancelled attempt terminal", ended, 60)
        if attempt["attempt_id"] != attempt_id or attempt["termination"]["outcome"] == "succeeded":
            raise RuntimeError(f"cancel did not stop the attempt: {attempt}")
        left = ssh(WORKER, f'pgrep -af "sh -c .*s26 {token}" | grep -v pgrep || true').stdout.strip()
        if left:
            raise RuntimeError(f"command still runs on the worker after cancel: {left}")
        if self.starts(token) != {WORKER: 1}:
            raise RuntimeError(f"unexpected starts {self.starts(token)}")
        save(RUN / "evidence/lp-cancel.json", {"task": task, "attempt": attempt})
        return f"cancel stopped the worker's command (outcome {attempt['termination']['outcome']}); nothing left running"

    def doctor(self):
        p = self.belt("doctor", check=False, timeout=180)
        value = json.loads(p.stdout)["result"]
        checks = value["checks"]
        # Every check must be verified: a not_checked result is not a pass.
        bad = [c for c in checks if c["level"] in ("fail", "not_checked")]
        mine = [c for c in checks if c.get("host") == WORKER]
        if bad or not mine or p.returncode or not value.get("healthy"):
            raise RuntimeError(f"doctor exit {p.returncode}: failures {bad}; worker checks {len(mine)}")
        save(RUN / "evidence/lp-doctor.json", value)
        return f"doctor exit 0, healthy, {len(checks)} checks ({len(mine)} for w1), none failed or unchecked"

    def resident_restart(self):
        task, token, attempt_id = self.place_on_worker()
        controlled_service(_Shim(self), "stop")
        try:
            # The run keeps going on the worker while the parent is down.
            if self.starts(token) != {WORKER: 1}:
                raise RuntimeError("command is not running on the worker during the outage")
        finally:
            controlled_service(_Shim(self), "start")
        until("parent answers again", lambda: self.belt("ls", check=False).returncode == 0, 60)
        self.release(token)
        self.belt("wait", task, timeout=150)
        (attempt,) = self.attempts(task)
        if (attempt["attempt_id"] != attempt_id or attempt["collection_verdict"] != "complete"
                or (attempt["termination"] or {}).get("outcome") != "succeeded"
                or self.starts(token) != {WORKER: 1}):
            raise RuntimeError(f"the restarted parent did not follow the run: {attempt}")
        save(RUN / "evidence/lp-restart.json", {"task": task, "attempt": attempt})
        return "run kept going while the parent was stopped; restarted parent collected the same attempt, one start"

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
        for task in self.tasks:
            try:
                self.belt("cancel", task, timeout=15, check=False)
            except Exception:
                pass


class _Shim:
    """peers.controlled_service drives a Checks-like object for m1 only."""

    def __init__(self, parent):
        self.parent = parent

    def service(self, host, expected, kind="worker"):
        self.parent.service(expected)


def run():
    lp = Parent()
    try:
        lp.check("L01 setup and belt init on Linux", lambda: (lp.setup(), lp.init())[1])
        if lp.results[-1]["status"] == "PASS":
            lp.check("L02 add worker over container SSH", lp.add_worker)
        if all(r["status"] == "PASS" for r in lp.results):
            lp.check("L03 run on the other container, result and logs", lp.run_on_worker)
            lp.check("L04 belt exec from the Linux parent", lp.exec_on_worker)
            lp.check("L05 cancel a run", lp.cancel)
            lp.check("L06 belt doctor", lp.doctor)
            lp.check("L07 stop/start the parent resident", lp.resident_restart)
        else:
            lp.check("L03-L07", lambda: (_ for _ in ()).throw(Skip("setup failed; nothing ran")))
    finally:
        lp.close()
        save(RUN / "evidence/linux-parent-summary.json", lp.results)
    count = {k: sum(r["status"] == k for r in lp.results) for k in ("PASS", "SKIP", "FAIL")}
    print(f"Summary: {count['PASS']} PASS, {count['SKIP']} SKIP, {count['FAIL']} FAIL "
          f"of {len(lp.results)}; evidence: {RUN / 'evidence'}")
    return 1 if count["FAIL"] or count["SKIP"] else 0
