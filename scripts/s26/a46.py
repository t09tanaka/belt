"""Exercise ephemeral identity retirement through a real, removable OpenSSH tunnel."""
import json
import re
import shlex
import subprocess

from harness import BASE, CONFIG, LABEL, RUN, Checks, command, save, sha256, ssh, until


def require(value, detail):
    if not value:
        raise RuntimeError(detail)
    return value


def stop(child):
    if child is not None and child.poll() is None:
        child.terminate()
        child.wait(timeout=15)


def check(checks):
    manifest = json.loads((RUN / "resources.json").read_text())
    name = manifest["containers"][0]
    container = json.loads(command(["docker", "inspect", name]).stdout)[0]
    require(container["Config"]["Labels"].get(LABEL) == manifest["token"],
            "worker container is not owned by this run")
    require(container["Config"]["Hostname"] == "w1", "expected the fresh w1 guest")
    linux = BASE / "belt-linux-aarch64"
    command(["docker", "cp", linux, f"{name}:/usr/local/bin/belt"])
    key = "/home/belt/s26-ephemeral-key"
    command(["docker", "cp", RUN / "cluster.key", f"{name}:{key}"])
    command(["docker", "exec", name, "chown", "belt:belt", key])
    require(ssh("w1", "sha256sum /usr/local/bin/belt").stdout.split()[0] == sha256(linux),
            "guest binary differs from build")
    # Use a Unix forwarding socket so there is no extra host TCP port to allocate.
    endpoint = RUN / "a46.sock"
    config = RUN / "ssh_config"
    config.write_text("Host colab\n  HostKeyAlias colab-w1\n"
                      f"  ProxyCommand /usr/bin/nc -U {shlex.quote(str(endpoint))}\n"
                      + config.read_text())
    (RUN / "config/belt/config.toml").write_text(
        CONFIG.replace("stale_seconds=10", "stale_seconds=3\nlease_seconds=900")
        + '\n[[workers]]\nid="colab"\nssh="colab"\nephemeral=true\n')
    checks.start()
    until("authenticated coordinator after identity pin",
          lambda: checks.belt("top").get("resident_observed") is True, 60)
    # A resident includes its own local worker. Fill its only slot before
    # exposing the tunnel, then release it while the remote attempt is unknown.
    blocker = checks.belt("submit", "--", "sh", "-c",
                          'touch "$HOME/a46-blocker"; while ! test -e "$HOME/a46-release"; do sleep 1; done')
    until("local worker occupies its only slot", lambda: (RUN / "home/a46-blocker").exists(), 90)
    cluster = json.loads(checks.input.read_text())["expected_cluster"]
    bridge = agent = None

    def forward():
        endpoint.unlink(missing_ok=True)
        child = subprocess.Popen([RUN / "bin/ssh", "-nNT", "-o", "ExitOnForwardFailure=yes",
                                  "-L", f"{endpoint}:127.0.0.1:22", "w1"],
                                 stdout=subprocess.DEVNULL, stderr=tunnel_log)
        try:
            until("OpenSSH forwarding socket", lambda: child.poll() is None and endpoint.is_socket(), 15)
            return child
        except BaseException:
            stop(child)
            raise

    def start_agent(log):
        return subprocess.Popen([RUN / "bin/ssh", "-T", "--", "w1",
                                 f"exec belt worker --ephemeral --cluster {shlex.quote(cluster)} "
                                 f"--name colab --key-file {shlex.quote(key)}"],
                                stdin=subprocess.DEVNULL, stdout=log, stderr=log)

    def identity(log):
        match = re.search(r"started as instance ([0-9A-Z]{26})", log.read_text())
        return match.group(1) if match else None

    def count():
        script = 'if test -f "$HOME/s26-starts/a46"; then wc -l < "$HOME/s26-starts/a46"; else echo 0; fi'
        return int(ssh("w1", script).stdout.strip())

    def attempt():
        attempts = checks.views([task])[task]["attempts"]["items"]
        require(len(attempts) <= 1, f"unexpected duplicate attempts: {attempts}")
        return attempts[0] if attempts else {}

    evidence = RUN / "evidence"
    first = evidence / "a46-first-agent.log"
    second = evidence / "a46-second-agent.log"
    with (evidence / "a46-tunnel.log").open("w") as tunnel_log, first.open("w") as log1, second.open("w") as log2:
        try:
            bridge = forward()
            agent = start_agent(log1)
            first_id = until("first ephemeral identity", lambda: identity(first), 30)
            task, = checks.submit(600, 1, "a46")
            until("one command began through the SSH tunnel", lambda: count() == 1)
            placed = attempt()
            require(placed.get("worker_id") == "colab", f"wrong worker: {placed}")
            attempt_id = placed["attempt_id"]
            stop(bridge)
            until("unknown attempt while tunnel is down",
                  lambda: attempt().get("observation") == "unknown", 100)
            (RUN / "home/a46-release").touch()
            checks.belt("wait", blocker["run_id"], timeout=90)
            unknown = attempt()
            require(unknown["attempt_id"] == attempt_id and not unknown["termination"],
                    f"attempt ended or changed before forget: {unknown}")
            require(agent.poll() is None and count() == 1, "disconnect killed the agent or duplicated the command")
            forgotten = checks.belt("workers", "forget", "colab")
            require(forgotten["identity_retired"] and forgotten["ended"] == [attempt_id],
                    f"forget did not retire this attempt: {forgotten}")
            after = attempt()
            require(after["attempt_id"] == attempt_id and after["termination"]["outcome"] == "interrupted",
                    f"unexpected terminal record: {after}")
            require(agent.poll() is None, "old agent exited before reconnect")
            bridge = forward()
            require(agent.wait(timeout=90) == 0, "returning agent did not end cleanly")
            require("identity was retired" in first.read_text(), "old identity did not receive retirement notice")
            require(ssh("w1", 'test ! -e "$HOME/.belt"').returncode == 0,
                    "old agent did not remove its state")
            require(count() == 1 and attempt()["attempt_id"] == attempt_id,
                    "returning identity ran the command again")
            agent = start_agent(log2)
            next_id = until("fresh identity after restart", lambda: identity(second), 30)
            require(next_id != first_id, "ephemeral worker reused its identity")
            report = dict(task_id=task, attempt_id=attempt_id, unknown=unknown,
                          forgotten=forgotten, after_forget=after, command_start_count=count(),
                          first_instance=first_id, next_instance=next_id, linux_sha256=sha256(linux),
                          native_sha256=sha256(checks.binary))
            save(evidence / "a46.json", report)
            return f"one remote command, unknown until forget, old identity refused, fresh identity {next_id}"
        finally:
            (RUN / "home/a46-release").touch()
            stop(bridge)
            stop(agent)


def run():
    checks = Checks()
    try:
        checks.check("A46", lambda: check(checks))
    finally:
        checks.close()
        save(RUN / "evidence/summary.json", checks.results)
    return int(not all(result["pass"] for result in checks.results))
