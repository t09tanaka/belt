#!/usr/bin/env bash
set -euo pipefail
ssh-keygen -A
exec /sbin/init
