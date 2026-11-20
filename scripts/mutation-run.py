#!/usr/bin/env python3
"""Run every pure mutant against its selected causal tests, with evidence."""
import argparse
from collections import Counter
import json
import os
import re
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

from pure_modules import ROOT, modules, selectors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard", required=True)
    parser.add_argument("--jobs", type=int, default=int(os.environ.get("CARGO_BUILD_JOBS", os.cpu_count() or 1)))
    parser.add_argument("--output", default="schemata.out")
    parser.add_argument("--tests-only", action="store_true")
    args = parser.parse_args()
    k, n = map(int, args.shard.split("/"))
    if not 0 <= k < n or args.jobs < 1:
        parser.error("invalid shard or jobs")
    os.chdir(ROOT)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    # Never attach a previous successful report to this invocation's failure.
    for name in ("report.json", "selected-tests.txt", "selected-integration-tests.txt", "run.log"):
        (output / name).unlink(missing_ok=True)
    evidence_path = output / "selection.json"
    evidence = {"run_id": str(uuid.uuid4()), "started_unix": time.time(), "stage": "selection"}
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n")
    filters = selectors()
    # The config integration module is the existing fast causal suite for legacy,
    # parse and paths. No other integration module is selected by these filters.
    test_args = ["--lib", "--test", "it", "--", *filters]
    # List the actual selected suite before mutation; renamed/unmapped test modules
    # must fail instead of silently giving a green zero-test baseline.
    started = time.monotonic()
    listing = subprocess.run(["cargo", "test", *test_args, "--list"], text=True, stdout=subprocess.PIPE, check=True).stdout
    (output / "selected-tests.txt").write_text(listing)
    names = [line[:-6] for line in listing.splitlines() if line.endswith(": test")]
    it_listing = subprocess.run(["cargo", "test", "--test", "it", "--", *filters, "--list"], text=True, stdout=subprocess.PIPE, check=True).stdout
    (output / "selected-integration-tests.txt").write_text(it_listing)
    it_names = [line[:-6] for line in it_listing.splitlines() if line.endswith(": test")]
    if not it_names or any(not name.startswith("config::") for name in it_names):
        raise ValueError("integration selection must contain only the nonempty config:: causal suite")
    missing = [f for f in filters if not any(f in name for name in names)]
    if missing:
        raise ValueError(f"no selected tests for {missing}")
    command = ["cargo", "schemata"]
    for path in modules():
        command += ["--file", path]
    command += ["--shard", args.shard, "--jobs", str(args.jobs), "--timeout", "60", "-o", str(output), "--", *test_args]
    evidence.update(modules=modules(), selectors=filters, selected_test_count=len(names), selected_integration_count=len(it_names), command=command,
                    selection_seconds=time.monotonic() - started, stage="mutation")
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"pure selection: {len(modules())} modules, {len(names)} selected tests", flush=True)
    if args.tests_only:
        return subprocess.run(["cargo", "test", *test_args]).returncode
    version = subprocess.check_output(["cargo", "schemata", "--version"], text=True).strip()
    if version != "cargo-schemata 0.4.0":
        raise ValueError(f"expected cargo-schemata 0.4.0, got {version}")
    started = time.monotonic()
    # Keep Unix socket paths short. A mutation of PID classification must
    # never sweep another test process's live fixture. The unmodified driver
    # owns cleanup, including leftovers from killed or timed-out mutants.
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="belt-m-") as fixtures, (output / "run.log").open("w") as log:
        env = dict(os.environ, BELT_MUTATION_FIXTURE_PARENT=fixtures)
        result = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in result.stdout:
            log.write(line)
            log.flush()
            print(line, end="", flush=True)
        result.wait()
    evidence.update(version=version, elapsed_seconds=time.monotonic() - started, cargo_exit_code=result.returncode, stage="complete")
    text = (output / "run.log").read_text()
    for stage, pattern in {"prepare": r"built: .*\(([0-9.]+)s\)", "baseline": r"baseline: tests pass in ([0-9.]+)s", "trace": r"traced: .*\(([0-9.]+)s\)", "mutants_including_single_builds": r"mutants: run in ([0-9.]+)s"}.items():
        match = re.search(pattern, text)
        if match:
            evidence[stage + "_seconds"] = float(match[1])
    report = output / "report.json"
    valid = False
    if report.exists():
        data = json.loads(report.read_text())
        evidence["mutant_counts"] = dict(Counter(mutant["status"] for file in data.get("files", {}).values() for mutant in file["mutants"]))
        counts = evidence["mutant_counts"]
        valid = (set(data.get("files", {})) == set(modules())
                 and sum(counts.get(status, 0) for status in ("Killed", "CompileError")) > 0
                 and not (set(counts) - {"Killed", "CompileError", "Pending"})
                 and (n > 1 or not counts.get("Pending")))
    evidence["exit_code"] = result.returncode or (0 if valid else 1)
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n")
    if not valid:
        print("mutation report missing, incomplete, empty, survived, timed out or unknown", flush=True)
    return evidence["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
