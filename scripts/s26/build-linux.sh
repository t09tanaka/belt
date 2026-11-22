#!/usr/bin/env bash
# build-linux.sh               N from this working tree.
# build-linux.sh --upgrade REF  N, N+1 and a broken N+1, all from the Git ref
#                               REF (for the A57 checks; see README).
set -euo pipefail
# shellcheck source=scripts/s26/common.sh
source "$(dirname "$0")/common.sh"
safe_paths
TTC=$(tool task-traffic-controller)
mkdir -p "$S26_BASE/linux-cache"

# build SOURCE OUTPUT: one release build, copied to $S26_BASE/OUTPUT.
build() {
    # shellcheck disable=SC2016 # expanded by the container shell
    "$TTC" exec -- docker run --rm --platform "linux/$S26_ARCH" \
        --mount "type=bind,source=$1,target=/src/$2,readonly" \
        --mount "type=bind,source=$S26_BASE,target=/cache" \
        -e CARGO_HOME=/cache/linux-cache/cargo \
        -e CARGO_TARGET_DIR=/cache/linux-cache/target \
        -e S26_TARGET="$S26_MACHINE-unknown-linux-gnu" \
        -e S26_OUTPUT="$2" \
        -w "/src/$2" rust:1.93.0-bookworm bash -euo pipefail -c '
            cargo build --locked --release --target "$S26_TARGET" -p belt-cli --bin belt
            cp "/cache/linux-cache/target/$S26_TARGET/release/belt" "/cache/$S26_OUTPUT.tmp"
            chmod 755 "/cache/$S26_OUTPUT.tmp"
            mv "/cache/$S26_OUTPUT.tmp" "/cache/$S26_OUTPUT"
        '
}

case "${1:-}" in
    "")
        build "$S26_ROOT" belt-linux-${S26_MACHINE}
        ;;
    --upgrade)
        ref=${2:?usage: build-linux.sh --upgrade <git ref>}
        python3 "$S26_ROOT/scripts/s26/harness.py" variants "$ref"
        src="$S26_BASE/linux-cache/src"
        # The exports share one target directory, and Cargo fingerprints a path
        # package by its place in the workspace and by mtime, not by where it
        # sits: without a fresh mtime, the next export looks already built.
        for v in n:belt-linux-${S26_MACHINE} n1:belt-linux-${S26_MACHINE}-n1 \
            n1-broken:belt-linux-${S26_MACHINE}-n1-broken; do
            find "$src/${v%%:*}" -type f -exec touch {} +
            build "$src/${v%%:*}" "${v#*:}"
        done
        # The upgrade names versions by the binary's SHA-256.
        shasum -a 256 "$S26_BASE"/belt-linux-${S26_MACHINE} "$S26_BASE"/belt-linux-${S26_MACHINE}-n1 \
            "$S26_BASE"/belt-linux-${S26_MACHINE}-n1-broken
        ;;
    *)
        echo 'usage: build-linux.sh [--upgrade <git ref>]' >&2
        exit 2
        ;;
esac
