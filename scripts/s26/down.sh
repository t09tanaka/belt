#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=scripts/s26/common.sh
source "$(dirname "$0")/common.sh"
safe_paths
python3 "$S26_ROOT/scripts/s26/harness.py" down
