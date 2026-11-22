"""Exercise an administrator-registered systemd worker under its non-root user."""
import json
from pathlib import PurePosixPath


def verify(checks, run, binary, command, save, until):
    config = run / "config/belt/config.toml"
    text = config.read_text()
    config.write_text(text + '\n[[workers]]\nid="w1"\nssh="w1"\nservice_mode="system-user"\n')
    checks.start()
    install = ("workers", "add", "w1", "--ssh", "w1", "--install", "--binary", str(binary))
    staged = command([checks.binary, "--json", *install], cwd=checks.project,
                     env=checks.env, timeout=180, check=False)
    if staged.returncode == 0:
        raise RuntimeError("a new system worker was accepted without administrator registration")
    manifest = json.loads((run / "resources.json").read_text())
    name, = (c for c in manifest["containers"] if c.endswith("-w1"))
    label = command(["docker", "inspect", "--format",
                     '{{index .Config.Labels "io.belt.s26.run"}}', name]).stdout.strip()
    if label != manifest["token"]:
        raise RuntimeError("container label differs from this run")
    admin = "/usr/local/sbin/belt-s26-admin"
    command(["docker", "cp", binary, f"{name}:{admin}"])

    def root(*args):
        return command(["docker", "exec", name, *args]).stdout.strip()

    registered = json.loads(root(admin, "--json", "register-system-worker", "--account",
                                 "belt", "--config-dir", "/home/belt/.config/belt"))
    unit = PurePosixPath(registered["result"]["path"])
    if unit.parent != PurePosixPath("/etc/systemd/system") or unit.suffix != ".service":
        raise RuntimeError(f"unexpected systemd unit path {unit}")
    root("systemctl", "daemon-reload")
    root("systemctl", "enable", "--now", unit.name)
    checks.belt(*install, timeout=180)
    pid = root("systemctl", "show", "-p", "MainPID", "--value", unit.name)
    owner = root("ps", "-o", "user=", "-p", pid)
    if owner != "belt" or root("systemctl", "is-enabled", unit.name) != "enabled":
        raise RuntimeError(f"system worker is not enabled and running as belt (user={owner})")
    root("systemctl", "restart", unit.name)
    new_pid = root("systemctl", "show", "-p", "MainPID", "--value", unit.name)
    if new_pid == pid or not new_pid.isdigit() or int(new_pid) <= 1:
        raise RuntimeError("systemd did not start a new worker process")
    until("restarted system worker connected", lambda:
          checks.belt("top")["workers"].get("w1") == "connected", 60)
    probe = command([checks.binary, "--json", "doctor"], env=checks.env, cwd=checks.project,
                    timeout=180, check=False)
    report = json.loads(probe.stdout)["result"]
    worker_checks = {c["id"]: c for c in report["checks"] if c["host"] == "w1"}
    for required in ("reachable", "service_profile"):
        if worker_checks.get(required, {}).get("level") != "ok":
            raise RuntimeError(f"doctor did not validate w1 {required}: {worker_checks.get(required)}")
    failures = [c for key, c in worker_checks.items()
                if key != "environment_diff" and c["level"] == "fail"]
    if failures:
        raise RuntimeError(f"doctor failed on the system worker: {failures}")
    tasks = checks.submit(3, 4)
    views = checks.finish(tasks)
    if not any(v["attempts"]["items"][0]["worker_id"] == "w1" for v in views.values()):
        raise RuntimeError("system worker did not execute the submitted task")
    save(run / "evidence/system-worker.json",
         {"unit": str(unit), "user": owner, "pid_before": pid, "pid_after": new_pid,
          "doctor": worker_checks, "tasks": views})
    return f"{unit.name} enabled, serving work as {owner}"
