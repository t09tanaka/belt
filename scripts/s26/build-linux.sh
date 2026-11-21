#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=scripts/s26/common.sh
source "$(dirname "$0")/common.sh"
safe_paths
TTC=$(tool task-traffic-controller)
mkdir -p "$S26_BASE/linux-cache"
"$TTC" exec -- docker run --rm --platform linux/arm64 \
    --mount "type=bind,source=$S26_ROOT,target=/src,readonly" \
    --mount "type=bind,source=$S26_BASE,target=/cache" \
    -e CARGO_HOME=/cache/linux-cache/cargo \
    -e CARGO_TARGET_DIR=/cache/linux-cache/target \
    -w /src rust:1.93.0-bookworm bash -euo pipefail -c '
        cargo build --locked --release --target aarch64-unknown-linux-gnu -p belt-cli --bin belt
        cp /cache/linux-cache/target/aarch64-unknown-linux-gnu/release/belt /cache/belt-linux-aarch64.tmp
        chmod 755 /cache/belt-linux-aarch64.tmp
        mv /cache/belt-linux-aarch64.tmp /cache/belt-linux-aarch64
    '
