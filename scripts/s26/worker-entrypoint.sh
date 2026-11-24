#!/usr/bin/env bash
set -euo pipefail
ssh-keygen -A
# The image ships an empty /etc/machine-id. Give each container its own
# before PID 1 starts, so the sweep (which proves files by machine ID) has
# one; an image-wide ID would make every container look like the same host.
if [[ ! -s /etc/machine-id ]]; then
    # systemd-machine-id-setup derives the ID from the VM UUID, which every
    # container shares; take a random one instead.
    tr -d - < /proc/sys/kernel/random/uuid > /etc/machine-id
fi
exec /sbin/init
