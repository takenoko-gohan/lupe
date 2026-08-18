#!/usr/bin/env bash
# Entry point for the mitm container (runs as root).
#   1. Generate the CA (if it does not exist)
#   2. Start mitmdump in the foreground
#
# --set rawtcp=false is mandatory. With the default of true, traffic inside a CONNECT that
# cannot be parsed as HTTP is passed through as raw TCP without reaching request();
# policy.py defines no TCP message hook, so that would bypass its request policy.
# --set websocket=false is equally mandatory. With the default of true, frames after the
# 101 do not go through request(), and policy.py defines no WebSocket message hook, so a
# single GET upgrade would open a full-duplex channel.
# running() in policy.py also verifies both values and aborts startup if either differs
# from the expected value.
set -euo pipefail

/usr/local/bin/gen-ca.sh

echo "[entrypoint] starting mitmdump..."
exec mitmdump \
  --mode regular \
  --listen-host 0.0.0.0 \
  --listen-port 3128 \
  --set rawtcp=false \
  --set websocket=false \
  --set confdir=/private/ca \
  --set termlog_verbosity=warn \
  --set flow_detail=0 \
  -s /opt/proxy/policy.py
