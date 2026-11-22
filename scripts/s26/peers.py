"""S26 real-SSH acceptance for independent submitters and group key rotation."""
import json
import sys

from harness import Checks, RUN, USER_BUS, command, save, ssh, until


def listed(cli):
    ids = set()
    args = ["ls", "--limit", "64"]
    while True:
        result = cli(*args)
        page = result["page"]
        ids.update(run["run_id"] for run in page["items"])
        cursor = page["next_cursor"]
        if not cursor:
            return ids
        args = ["ls", "--limit", "64", "--cursor", cursor]


def controlled_service(checks, action):
    # Scope the systemd operation to the one fixture user's initialized resident.
    script = ('set -- "$HOME"/.config/systemd/user/io.github.t09tanaka.belt.local.*.service; '
              '[ "$#" = 1 ] && [ -f "$1" ] || exit 90; '
              'exec systemctl --user ' + action + ' "$(basename "$1")"')
    ssh("m1", USER_BUS + script, timeout=80)
    checks.service("m1", "inactive" if action == "stop" else "active", kind="local")


def peer_outage(checks):
    # Two submission authorities place real commands on w1. Release the m1
    # command during the outage so the worker slot is free for the host parent.
    remote = checks.start_remote_attempt()
    task, token, original = remote
    m1_run = checks.m1_belt("show", task)["task"]["run_id"]
    local = checks.belt("submit", "--requires", "linux", "--", "sh", "job.sh", "5")
    checks.tasks.append(local["task_id"])
    host_run = local["run_id"]
    host_ls, peer_ls = listed(checks.belt), listed(checks.m1_belt)
    if (host_run not in host_ls or host_run in peer_ls
            or m1_run not in peer_ls or m1_run in host_ls):
        raise RuntimeError("concurrently submitted runs have incorrect owner listings")
    save(RUN / "evidence/a51-run-owners.json",
         {"host_run": host_run, "m1_run": m1_run, "host_ls": sorted(host_ls),
          "m1_ls": sorted(peer_ls)})
    controlled_service(checks, "stop")
    try:
        ssh("w1", f'touch "$HOME/s26-release-{token}"')
        tasks = []
        for _ in range(4):
            submission = checks.belt("submit", "--requires", "linux", "--", "sh", "job.sh", "4")
            tasks.append(submission["task_id"])
            checks.tasks.append(submission["task_id"])
        results = checks.finish(tasks)
        on_w1 = [view["attempts"]["items"][0]["attempt_id"] for view in results.values()
                 if view["attempts"]["items"][0]["worker_id"] == "w1"]
        if not on_w1:
            raise RuntimeError("no independent host task completed on w1 while m1 was stopped")
        checks.service("m1", "inactive", kind="local")
    finally:
        controlled_service(checks, "start")
    checks.finish_remote_attempt(remote, "a54-resumed-attempt.json")
    checks.finish([local["task_id"]])
    save(RUN / "evidence/a54-other-member.json",
         {"unavailable_submitter": "m1", "shared_worker": "w1", "m1_attempt": original,
          "host_attempts_completed_during_outage": on_w1})
    return "each parent listed only its own run; independent host work completed on w1 during m1 outage"


def key_distribution(checks):
    """Rotate across the fixture's installed members over their real SSH connections."""
    before = checks.belt("group")
    remote = checks.start_remote_attempt()
    rotated = checks.belt("group", "rotate-key", "--skip", "w2")
    fingerprint = rotated["fingerprint"]
    if before["current"] and fingerprint == before["current"]["fingerprint"]:
        raise RuntimeError("rotation retained the previous fingerprint")
    remaining = checks.m1_belt("group")
    if remaining["current"]["fingerprint"] != fingerprint:
        raise RuntimeError("m1 did not switch to the newly distributed key")
    checks.finish_remote_attempt(remote, "a53-live-attempt.json")
    worker_unit = (USER_BUS + 'set -- "$HOME"/.config/systemd/user/io.github.t09tanaka.belt.worker.*.service; '
                   '[ "$#" -eq 1 ] && [ -f "$1" ] || exit 90; unit=$(basename "$1"); ')
    status = 'systemctl --user show "$unit" -p MainPID --value'
    previous_pid = int(ssh("w1", worker_unit + status).stdout.strip())
    ssh("w1", worker_unit + 'systemctl --user restart "$unit"')
    checks.service("w1", "active")
    current_pid = int(ssh("w1", worker_unit + status).stdout.strip())
    if min(previous_pid, current_pid) <= 0 or previous_pid == current_pid:
        raise RuntimeError("w1 worker service failed to restart after rotation")
    until("w1 reconnected after restart", lambda: any(
        m["name"] == "w1" and m["connection"] == "connected"
        for m in checks.belt("group")["members"]), 120)
    restarted_work = checks.start_remote_attempt()
    checks.finish_remote_attempt(restarted_work, "a53-restarted-worker.json")
    sent = checks.belt("group", "send-key", "w2")
    if sent["fingerprint"] != fingerprint or not sent["identity_regenerated"]:
        raise RuntimeError("w2 did not install the current key with a fresh identity")
    def connected():
        return any(m["name"] == "w2" and m["connection"] == "connected"
                   for m in checks.belt("group")["members"])
    until("w2 reconnected after send-key", connected, 120)
    removed = checks.belt("group", "rotate-key", "--remove", "w2")
    final = checks.belt("group")
    if removed["fingerprint"] == fingerprint or any(m["name"] == "w2" for m in final["members"]):
        raise RuntimeError("removal failed to advance the group key and unregister w2")
    if not any("authorized_keys" in s for s in removed["guidance"]):
        raise RuntimeError("rotation omitted SSH key removal guidance")
    if not any(w["name"] == "w2" and w["code"] == "removed_member_ssh_reachable"
               for w in final["warnings"]):
        raise RuntimeError("group did not warn about the removed member's SSH access")
    doctor = command([checks.binary, "--json", "doctor"], env=checks.env,
                     cwd=checks.project, timeout=180, check=False)
    notices = [item for item in json.loads(doctor.stdout)["result"]["checks"]
               if item["host"] == "w2" and item["id"] == "removed_member_ssh_reachable"]
    if len(notices) != 1 or notices[0]["level"] != "notice":
        raise RuntimeError("doctor did not report the removed member's SSH access")
    save(RUN / "evidence/a53-key-lifecycle.json",
         {"distributed": fingerprint, "sent": sent["fingerprint"],
          "w1_service_pids": [previous_pid, current_pid], "fresh_identity": sent["identity_regenerated"], "after_removal": final,
          "guidance": removed["guidance"], "doctor": notices[0]})
    return "retained attempt completed once, skipped w2 rejoined via send-key, removed w2 reported reachable"


def main():
    checks = Checks()
    try:
        checks.start()
        checks.install("w1")
        checks.install("w2")
        checks.check("M1 init member", checks.m1)
        checks.connect_m1_to_w1()
        checks.check("A51 and A54", lambda: peer_outage(checks))
        checks.check("A53 rotation, recovery and removal", lambda: key_distribution(checks))
    finally:
        checks.close()
        save(RUN / "evidence/peer-summary.json", checks.results)
    return 0 if all(check["status"] == "PASS" for check in checks.results) else 1


if __name__ == "__main__":
    sys.exit(main())
