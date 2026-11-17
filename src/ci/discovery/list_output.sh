set -eu
umask 077
limit=$1
shift
dir=$(mktemp -d "${TMPDIR:-/tmp}/rtr-discovery.XXXXXXXX")
trap 'rm -f "$dir/out.pipe" "$dir/err.pipe" "$dir/out" "$dir/err"; rmdir "$dir"' EXIT
trap 'exit 1' HUP INT TERM
mkfifo "$dir/out.pipe" "$dir/err.pipe"
head -c "$((limit + 1))" <"$dir/out.pipe" >"$dir/out" &
out_pid=$!
head -c "$((limit + 1))" <"$dir/err.pipe" >"$dir/err" &
err_pid=$!
status=0
"$@" >"$dir/out.pipe" 2>"$dir/err.pipe" || status=$?
wait "$out_pid"
wait "$err_pid"
[ "$status" -eq 0 ] || exit "$status"
out=$(wc -c <"$dir/out")
err=$(wc -c <"$dir/err")
[ "$((out + err))" -le "$limit" ] || exit 1
printf 'rtr-list-v1 %d %d\n' "$out" "$err"
cat "$dir/out" "$dir/err"
