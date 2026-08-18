#!/usr/bin/env bash
# Import the proxy CA into the system trust store. This needs root, so sudoers grants the
# cursor user NOPASSWD access to this script only
# (no sudo is granted for general-purpose file placement commands such as install or cp).
set -euo pipefail

SRC=/shared/ca/proxy-ca.crt
if [[ ! -f "$SRC" ]]; then
  echo "[trust-ca] $SRC not found (check that mitm has generated the CA)" >&2
  exit 1
fi

install -m 0644 "$SRC" /usr/local/share/ca-certificates/proxy-ca.crt
update-ca-certificates
