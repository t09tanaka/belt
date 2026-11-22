"""A34 upgrade and rollback on an enrolled worker over the real SSH transport."""
import json
import shlex
import shutil


def verify(checks, run, original, host, command, ssh, sha256, save):
    # Linux ignores trailing ELF bytes. Keep the CLI version and make a second
    # build identity so the normal parent and worker version check still applies.
    candidate = run / "worker-upgrade-candidate"
    shutil.copyfile(original, candidate)
    with candidate.open("ab") as out:
        out.write(b"\nS26 worker upgrade candidate\n")
    candidate.chmod(0o700)
    before, after = sha256(original), sha256(candidate)
    def installed(digest):
        return ssh(host, 'p="$HOME/.config/belt/bin/' + digest
                   + '/belt"; stat -c "%i %s %a" "$p" && sha256sum "$p"').stdout

    kept = installed(before)
    probe = run / "upgrade-health-ssh"
    probe.mkdir(mode=0o700)
    wrapper = probe / "ssh"
    transport = shlex.quote(str(run / "bin/ssh"))
    # Only the invoking upgrade CLI uses this wrapper. All calls still go to
    # the harness SSH daemon, except the health probe of the switched build.
    wrapper.write_text(
        "#!/bin/sh\n" + 'case "$*" in\n  *--stdio*)\n'
        + "    current=$(" + transport + " " + shlex.quote(host)
        + " " + shlex.quote('readlink "$HOME/.config/belt/bin/current"')
        + ") || exit 1\n" + '    case "$current" in *' + after
        + ') exit 255 ;; esac\n    ;;\nesac\nexec ' + transport + ' "$@"\n')
    wrapper.chmod(0o700)
    args = ("workers", "upgrade", host, "--binary", str(candidate))
    env = dict(checks.env, PATH=f"{probe}:{checks.env['PATH']}")
    failed = command([checks.binary, "--json", *args], env=env, cwd=checks.project,
                     check=False, timeout=300)
    error = json.loads(failed.stdout).get("error", {})
    if not failed.returncode or "rolled back to version" not in error.get("message", ""):
        raise RuntimeError(f"the failed bridge check did not confirm rollback: {error}")
    if installed(before) != kept:
        raise RuntimeError("rollback replaced the original executable")
    switched = checks.belt(*args, timeout=300)
    repeated = checks.belt(*args, timeout=300)
    if (switched["outcome"], switched["from"], switched["to"]) != ("upgraded", before, after):
        raise RuntimeError(f"worker did not switch from {before} to {after}")
    if repeated["outcome"] != "already_current" or installed(before) != kept:
        raise RuntimeError("repeat upgrade failed or replaced the original executable")
    restored = checks.belt("workers", "upgrade", host, "--binary", str(original), timeout=300)
    if restored["to"] != before or not restored["status"]["ready"]:
        raise RuntimeError("worker was not serving its original build after verification")
    save(run / "evidence/worker-upgrade.json",
         {"host": host, "from": before, "to": after, "rollback": error["message"],
          "upgrade": switched["outcome"], "repeat": repeated["outcome"]})
    return f"{host} rolled back, upgraded on retry and kept the original executable"
