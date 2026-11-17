#!/usr/bin/env bash
# Delete the .profraw files that llvm-profdata cannot read (an instrumented
# process killed while writing its profile leaves a truncated one), naming
# each. Healthy files are kept. More than MAX_CORRUPT_PERCENT (default 5) of
# all files corrupt, or no readable file at all, is a failure.
set -euo pipefail
cd "$(dirname "$0")/.."
dir=$(cargo llvm-cov show-env --sh 2>/dev/null | sed -nE "s/^export CARGO_LLVM_COV_TARGET_DIR='?([^']*)'?$/\1/p")/llvm-cov-target
[ -d "$dir" ] || { echo "no coverage target dir: $dir" >&2; exit 1; }
prof=$(find "$(rustc --print sysroot)/lib/rustlib" -name llvm-profdata -type f | head -n 1)
[ -n "$prof" ] || { echo "llvm-profdata not found (rustup component add llvm-tools)" >&2; exit 1; }
max=${MAX_CORRUPT_PERCENT:-5}
total=0
corrupt=0
while IFS= read -r -d '' f; do
  total=$((total + 1))
  if ! "$prof" show "$f" >/dev/null 2>&1; then
    echo "dropping corrupt profile: ${f##*/} ($(wc -c <"$f" | tr -d ' ') bytes)" >&2
    rm -f "$f"
    corrupt=$((corrupt + 1))
  fi
done < <(find "$dir" -name '*.profraw' -type f -print0)
echo "profraw files: $total, corrupt dropped: $corrupt" >&2
if [ "$total" -eq 0 ] || [ "$corrupt" -eq "$total" ]; then
  echo "no readable profile data" >&2
  exit 1
fi
if [ $((corrupt * 100)) -gt $((total * max)) ]; then
  echo "too many corrupt profiles ($corrupt of $total, limit ${max}%)" >&2
  exit 1
fi
