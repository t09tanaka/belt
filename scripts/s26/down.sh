#!/usr/bin/env bash
# down.sh         remove this run's containers, network and run directory;
#                 the build caches and the image stay for the next build.
# down.sh --all   also remove the build caches, the built binaries and the
#                 worker image of this architecture.
set -euo pipefail
# shellcheck source=scripts/s26/common.sh
source "$(dirname "$0")/common.sh"
all=0
case "${1:-}" in
    "") ;;
    --all) all=1 ;;
    *) echo "usage: $0 [--all]" >&2; exit 2 ;;
esac
safe_paths
python3 "$S26_ROOT/scripts/s26/harness.py" down
if [[ "$all" == 1 ]]; then
    # The harness's portool project: give its ports back before its files go.
    if [[ -f "$S26_BASE/resources/.portool.toml" ]]; then
        (cd "$S26_BASE/resources" && "$(tool portool)" release)
    fi
    # Only the names the build scripts create; harness.lock stays (it is flocked).
    for name in linux-cache resources native; do
        rm -rf "${S26_BASE:?}/$name"
    done
    rm -f "${S26_BASE:?}"/belt-linux-"$S26_MACHINE" \
        "${S26_BASE:?}"/belt-linux-"$S26_MACHINE"-n1 \
        "${S26_BASE:?}"/belt-linux-"$S26_MACHINE"-n1-broken
    if docker image inspect "$S26_IMAGE" >/dev/null 2>&1; then
        docker image rm "$S26_IMAGE" >/dev/null
    fi
    echo "Removed the build caches, binaries and image $S26_IMAGE."
fi
