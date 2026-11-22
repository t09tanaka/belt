"""Run the same committed project CI with the parent alone and with two workers."""
import hashlib
import json
import sqlite3
import time
from pathlib import Path


def step_times(state, attempt_id):
    directory = state / "results" / attempt_id
    receipt = json.loads((directory / "result.json").read_text())
    artifact = next(a for a in receipt["artifacts"] if a["name"] == "belt-ci-task-result.json")
    algorithm, digest = artifact["digest"].split(":", 1)
    if algorithm != "sha256" or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise RuntimeError("invalid digest for the collected CI result")
    data = (directory / "artifacts" / digest).read_bytes()
    if hashlib.sha256(data).hexdigest() != digest:
        raise RuntimeError("collected CI result does not match its receipt")
    return [{"job": job["job"], "step_seconds": sum(step["secs"] for step in job["steps"]),
             "timing": job.get("time")} for job in json.loads(data)["jobs"]]


def measure(checks, commit, state):
    started = time.monotonic()
    result = checks.belt("ci", "--no-status", "--snapshot", "commit", "--ref", commit,
                         timeout=3600)
    elapsed = time.monotonic() - started
    if result["state"] != "settled" or not result["jobs"]:
        raise RuntimeError("project CI returned no settled jobs")
    outcomes = []
    attempts = []
    for job in result["jobs"]:
        if job.get("part") == "job":
            continue  # Aggregate of shards which are listed separately.
        outcomes.append((job["plan_task"], job.get("job") or "", job["status"], job.get("skipped") or ""))
        if job["status"] == "skipped":
            continue
        if job["status"] != "success":
            raise RuntimeError(f"project CI job did not succeed: {job}")
        page = checks.belt("show", job["task_id"], "--limit", "64")["attempts"]
        if page["next_cursor"] or len(page["items"]) != 1:
            raise RuntimeError(f"expected exactly one attempt for {job['plan_task']}")
        attempt = page["items"][0]
        if attempt["collection_verdict"] != "complete":
            raise RuntimeError(f"incomplete collection for {job['plan_task']}")
        attempts.append({"job": job["plan_task"], "worker": attempt["worker_id"],
                         "attempt_id": attempt["attempt_id"], "receipts": attempt["receipts"],
                         "steps": step_times(state, attempt["attempt_id"]),
                         "placed_ms": attempt["created_at_ms"],
                         "terminal_ms": attempt["terminal_at_ms"],
                         "collected_ms": attempt["collection_closed_at_ms"]})
    if not attempts:
        raise RuntimeError("project CI executed no jobs")
    run_id = result["snapshot"]["target"]["id"]
    costs = state / "parent/stats/costs-v1.sqlite"
    with sqlite3.connect(costs.as_uri() + "?mode=ro", uri=True) as db:
        transfers = [r[0] for r in db.execute(
            "SELECT duration FROM costs WHERE receipt GLOB ? ORDER BY receipt",
            (f"transfer:{run_id}:*",))]
    return {"transfer_samples_ms": transfers, "elapsed_seconds": elapsed, "outcomes": sorted(outcomes),
            "attempts": attempts, "plan_digest": result["plan_digest"],
            "run_id": run_id}


def compare(checks, repo, run, command, save):
    source = Path(repo).resolve(strict=True)
    project = run / "comparison-project"
    command(["git", "clone", "--no-local", "--", source, project], env=checks.env)
    commit = command(["git", "rev-parse", "HEAD"], cwd=project, env=checks.env).stdout.strip()
    checks.project = project
    checks.start()
    evidence = {"commit": commit, "runs": {}}
    for topology in ("parent", "group"):
        if topology == "group":
            for host in ("w1", "w2"):
                checks.install(host)
        for cache in ("first", "repeat"):
            sample = measure(checks, commit, run / "state")
            evidence["runs"][f"{topology}-{cache}"] = sample
            save(run / "evidence/project-ci.json", evidence)
            workers = {a["worker"] for a in sample["attempts"]}
            if topology == "parent" and workers != {"local"}:
                raise RuntimeError(f"parent-only run used {workers}")
            if topology == "group" and len(workers) < 2:
                raise RuntimeError(f"distributed run placed all jobs on {workers}")
            baseline = evidence["runs"]["parent-first"]
            if sample["outcomes"] != baseline["outcomes"]:
                raise RuntimeError("project CI outcomes differ across runs")
            print(f"{topology}-{cache}: {sample['elapsed_seconds']:.2f}s on {sorted(workers)}",
                  flush=True)
    for cache in ("first", "repeat"):
        samples = evidence["runs"]
        evidence[f"{cache}_speedup"] = (samples[f"parent-{cache}"]["elapsed_seconds"]
                                        / samples[f"group-{cache}"]["elapsed_seconds"])
    save(run / "evidence/project-ci.json", evidence)
    return f"four CI runs at {commit}; measurements in evidence/project-ci.json"
