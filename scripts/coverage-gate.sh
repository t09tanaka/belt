#!/usr/bin/env bash
# 100% line coverage for the pure modules, measured over the whole test suite.
# Add pure modules to scripts/pure-modules.txt (also used by mutation).
set -euo pipefail
cd "$(dirname "$0")/.."
# The report path stays local even when compilation uses CARGO_TARGET_DIR.
mkdir -p target
# The test run's stdout is where the harness names a failing test, so it is
# kept in a file: shown (the failed tests and their output) when the run
# fails, dropped when it passes.
log=$(mktemp)
trap 'rm -f "$log"' EXIT
# Measure first, report after: a test may stop an instrumented child (the
# daemon, a worker, a supervisor) while it is still writing its .profraw at
# exit, and one such truncated file makes the whole merge fail. The corrupt
# files are named and dropped between the two steps, within a bound.
if ! cargo llvm-cov --no-report >"$log"; then
  echo "coverage run failed" >&2
  grep -E '^test .* \.\.\. FAILED$' "$log" >&2 || true
  # each "failures:" section: the output of every failed test, then its list
  awk '/^failures:$/ { show = 1 } show { print } /^test result:/ { show = 0 }' "$log" >&2
  # no failed test in the log: the build or the harness failed, say how
  grep -q '^test result: FAILED' "$log" || tail -n 40 "$log" >&2
  exit 1
fi
scripts/drop-corrupt-profraw.sh
if ! cargo llvm-cov report --json --output-path target/llvm-cov.json >"$log" 2>&1; then
  echo "coverage report failed" >&2
  tail -n 40 "$log" >&2
  exit 1
fi
python3 - <<'PY'
import json, os, sys
root = os.getcwd()
data = json.load(open("target/llvm-cov.json"))
from sys import path
path.insert(0, "scripts")
from pure_modules import modules
want = set(modules())
seen = set()
bad = []
for f in data["data"][0]["files"]:
    rel = os.path.relpath(f["filename"], root)
    if rel not in want:
        continue
    seen.add(rel)
    lines = f["summary"]["lines"]
    print(f"{rel}: {lines['covered']}/{lines['count']} lines ({lines['percent']:.1f}%)")
    if lines["covered"] != lines["count"]:
        bad.append(rel)
missing = want - seen
if missing:
    print("not measured:", ", ".join(sorted(missing)))
    sys.exit(1)
if bad:
    print("below 100%:", ", ".join(bad))
    sys.exit(1)
print(f"{len(want)} pure modules at 100%")
PY
