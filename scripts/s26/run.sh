#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=scripts/s26/common.sh
source "$(dirname "$0")/common.sh"
safe_paths
case "${1:-}" in
    "") driver=("$S26_ROOT/scripts/s26/harness.py" run) ;;
    --a46) driver=("$S26_ROOT/scripts/s26/harness.py" a46) ;;
    --peers) driver=("$S26_ROOT/scripts/s26/peers.py") ;;
    *) echo "usage: scripts/s26/run.sh [--a46|--peers]" >&2; exit 2 ;;
esac
if (( $# > 1 )); then
    echo "usage: scripts/s26/run.sh [--a46|--peers]" >&2
    exit 2
fi
TTC=$(tool task-traffic-controller)
PORTOOL=$(tool portool)
# Resolve host tools before giving the parent its private HOME.
export S26_FAST_WORKTREE
S26_FAST_WORKTREE=$(tool fast-worktree)
: "${S26_BELT:?Set S26_BELT to an absolute path to the native Mac belt binary}"
cd "$S26_BASE/resources"
"$PORTOOL" sync --quiet
"$PORTOOL" check
exec "$TTC" exec -- "$PORTOOL" exec -- python3 "${driver[@]}"
