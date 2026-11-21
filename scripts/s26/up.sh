#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=scripts/s26/common.sh
source "$(dirname "$0")/common.sh"
safe_paths
PORTOOL=$(tool portool)
TTC=$(tool task-traffic-controller)
[[ ! -e "$S26_RUN" ]] || { echo 'Run exists; run down.sh first.' >&2; exit 1; }
docker info >/dev/null
mkdir -p "$S26_BASE/resources"
# A harness-owned Git root keeps generated portool files under target/s26.
# The normal shared portool ledger still coordinates with other worktrees.
git -C "$S26_BASE/resources" init -q
cp "$S26_ROOT/scripts/s26/ports.toml" "$S26_BASE/resources/.portool.toml"
cd "$S26_BASE/resources"
"$PORTOOL" sync --quiet
"$PORTOOL" check
"$TTC" exec -- docker build --platform linux/arm64 \
    -t belt-s26-worker:debian12.9-fw0.4.0 "$S26_ROOT/scripts/s26"
exec "$PORTOOL" exec --strict -- python3 "$S26_ROOT/scripts/s26/harness.py" up
