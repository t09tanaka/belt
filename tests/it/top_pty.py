"""Black-box terminal acceptance for belt top (Linux and macOS)."""
import contextlib
import fcntl
import json
import os
from pathlib import Path
import re
import select
import struct
import subprocess
import sys
import termios
import time

belt = sys.argv[1]
ENTER = b"\x1b[?1049h"
LEAVE = b"\x1b[?1049l"
CSI = re.compile(rb"\x1b\[[0-?]*[ -/]*[@-~]")


def visible(transcript):
    """Apply cursor moves and erase commands to ratatui's incremental frames."""
    rows = {}
    y = x = 0
    pos = 0
    data = bytes(transcript)
    while pos < len(data):
        if data[pos:pos + 2] == b"\x1b[":
            seq = CSI.match(data, pos)
            if seq is None:
                break  # partial escape sequence at end of a PTY read
            op = seq.group()[-1:]
            args = seq.group()[2:-1].split(b";")
            numbers = [int(arg) if arg.isdigit() else 0 for arg in args]
            n = numbers[0] or 1
            if op in (b"H", b"f"):
                y, x = n - 1, (numbers[1] or 1) - 1 if len(numbers) > 1 else 0
            elif op == b"G":
                x = n - 1
            elif op in (b"C", b"D"):
                x = max(0, x + (n if op == b"C" else -n))
            elif op in (b"A", b"B"):
                y = max(0, y + (n if op == b"B" else -n))
            elif op == b"J" and n == 2:
                rows.clear()
            elif op == b"K":
                rows[y] = {c: v for c, v in rows.get(y, {}).items() if c < x}
            pos = seq.end()
        else:
            c = data[pos]
            pos += 1
            if c == 13:
                x = 0
            elif c == 10:
                y += 1
            elif 32 <= c < 127:
                rows.setdefault(y, {})[x] = c
                x += 1
            elif c >= 192:
                x += 1  # each non-ASCII glyph used by the display occupies one column
    return b"\n".join(bytes(row.get(i, 32) for i in range(max(row, default=-1) + 1))
                       for _, row in sorted(rows.items()))


def cli(*args):
    result = subprocess.run([belt, *args], capture_output=True, timeout=45)
    assert result.returncode == 0, (args, result.stdout, result.stderr)
    return json.loads(result.stdout)


class Screen:
    def __init__(self, master, slave, process):
        self.master = master
        self.slave = slave
        self.process = process
        self.transcript = bytearray()

    def expect(self, matches, description):
        captured = bytearray()
        deadline = time.monotonic() + 35
        while time.monotonic() < deadline:
            ready, _, _ = select.select([self.master], [], [], 0.2)
            if ready:
                chunk = os.read(self.master, 65536)
                captured.extend(chunk)
                self.transcript.extend(chunk)
                if matches(bytes(captured)):
                    return bytes(captured)
            if self.process.poll() is not None:
                break
        raise AssertionError((description, self.process.poll(), bytes(captured[-4000:])))

    def resize(self, cols):
        fcntl.ioctl(self.slave, termios.TIOCSWINSZ, struct.pack("HHHH", 35, cols, 0, 0))


@contextlib.contextmanager
def terminal():
    master, slave = os.openpty()
    process = None
    try:
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 35, 220, 0, 0))

        def take_terminal():
            os.setsid()
            fcntl.ioctl(0, termios.TIOCSCTTY, 0)

        # macOS revokes the slave when the controlling session leader exits.
        # A caller shell compares its modes while that session still exists.
        check_modes = ('before=$(stty -g) || exit 91; "$@"; status=$?; '
                       'after=$(stty -g) || exit 92; '
                       '[ "$after" = "$before" ] || exit 93; exit "$status"')
        process = subprocess.Popen(["/bin/sh", "-c", check_modes, "caller",
                                    belt, "top", "--interval", "0.2"],
                                   stdin=slave, stdout=slave, stderr=slave,
                                   preexec_fn=take_terminal)
        screen = Screen(master, slave, process)
        frame = screen.expect(lambda data: ENTER in data and b"top" in data.lower(),
                              "enter alternate screen and draw top")
        assert not (termios.tcgetattr(slave)[3] & termios.ICANON), "interactive mode"
        yield screen, frame
        status = process.wait(timeout=10)
        assert status == 0, ("top exit or failed to restore caller termios", status)
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        os.close(slave)
        os.close(master)


def quit_top(screen, key):
    os.write(screen.master, key)
    screen.expect(lambda data: LEAVE in data, "leave alternate screen")


def main():
    bad = subprocess.run([belt, "top"], stdin=subprocess.DEVNULL, capture_output=True, timeout=10)
    assert bad.returncode != 0 and ENTER not in bad.stdout and b"terminal" in bad.stderr.lower()
    root = Path(os.environ["HOME"])
    marker, release = root / "command-started", root / "release-command"
    shell = 'touch "$1"; while [ -d "$3" ] && [ ! -f "$2" ]; do sleep 0.1; done'
    try:
        submitted = cli("--json", "submit", "--", "/bin/sh", "-c", shell, "sh",
                        str(marker), str(release), str(root), "private-argv-sentinel")["result"]
        task = submitted["task_id"].encode()
        deadline = time.monotonic() + 35
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert marker.exists(), "worker command started"
        with terminal() as (screen, _):
            def running():
                page = visible(screen.transcript)
                return (b"Active (1)" in page and task in page and
                        submitted["run_id"].encode() in page and b"persistent-local" in page)

            if not running():
                screen.expect(lambda _: running(), "live task, run and worker in active section")
            snapshot = cli("--json", "top")["result"]
            assert snapshot["resident_observed"]
            rows = [row for row in snapshot["tasks"] if row["task_id"] == submitted["task_id"]]
            assert len(rows) == 1 and rows[0]["run_id"] == submitted["run_id"]
            assert rows[0]["worker_id"] == "persistent-local" and rows[0]["command"] == "sh"
            assert b"sh" in visible(screen.transcript)
            assert "private-argv-sentinel" not in json.dumps(snapshot)
            assert b"private-argv-sentinel" not in screen.transcript
            release.touch()
            cli("--json", "wait", submitted["run_id"])

            def completed():
                page = visible(screen.transcript)
                return b"Active (0)" in page and task not in page

            # No key, resize or restart triggers this update.
            screen.expect(lambda _: completed(), "active task disappears after completion")
            screen.resize(200)
            screen.expect(lambda data: b"belt" in CSI.sub(b"", data)
                          and completed(), "redraw the empty table after resize")
            quit_top(screen, b"q")
        for key in (b"\x1b", b"\x03"):
            with terminal() as (screen, _):
                quit_top(screen, key)
    finally:
        release.touch()


if __name__ == "__main__":
    main()
