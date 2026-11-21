#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=scripts/s26/common.sh
source "$(dirname "$0")/common.sh"
safe_paths
TTC=$(tool task-traffic-controller)
PORTOOL=$(tool portool)
# Resolve host tools before giving the parent its private HOME.
export S26_FAST_WORKTREE
S26_FAST_WORKTREE=$(tool fast-worktree)
: "${S26_BELT:?Set S26_BELT to an absolute path to the native Mac belt binary}"
cd "$S26_BASE/resources"
"$PORTOOL" sync --quiet
"$PORTOOL" check
exec "$TTC" exec -- "$PORTOOL" exec -- python3 "$S26_ROOT/scripts/s26/harness.py" run
