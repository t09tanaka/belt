#!/usr/bin/env bash
set -euo pipefail

S26_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
# A long checkout path overflows the 104-byte macOS socket limit; set S26_BASE to a short directory then.
S26_ARCH="${S26_ARCH:-arm64}"
case "$S26_ARCH" in
    arm64) S26_MACHINE=aarch64; default_base="$S26_ROOT/target/s26" ;;
    amd64) S26_MACHINE=x86_64; default_base="$S26_ROOT/target/s26-amd64" ;;
    *) echo "Unsupported S26_ARCH: $S26_ARCH (expected arm64 or amd64)" >&2; exit 2 ;;
esac
S26_IMAGE="belt-s26-worker:debian12.9-fw0.4.0-$S26_ARCH"
export S26_ARCH S26_MACHINE S26_IMAGE
S26_BASE="${S26_BASE:-$default_base}"
S26_RUN="$S26_BASE/run"
export S26_ROOT S26_BASE S26_RUN
umask 077

tool() {
    if command -v "$1" >/dev/null 2>&1; then command -v "$1"
    elif [[ -x "$HOME/.cargo/bin/$1" ]]; then printf '%s\n' "$HOME/.cargo/bin/$1"
    else printf 'Required tool missing: %s\n' "$1" >&2; return 1
    fi
}

# Reject symlinks before any mkdir, mount or recursive removal.
safe_paths() {
    python3 - "$S26_BASE" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
for q in [p, *p.parents]:
    if q.is_symlink():
        raise SystemExit(f"Refusing symlink: {q}")
for name in ('run', 'resources', 'linux-cache', 'harness.lock'):
    q = p / name
    if q.is_symlink():
        raise SystemExit(f"Refusing symlink: {q}")
PY
}
