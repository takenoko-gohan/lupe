#!/usr/bin/env bash
# Regression suite for the egress policy. Run it from the host.
#
#   .devcontainer/tests/regression-suite.sh
#
# If cursor is not running, the stack is started and brought down afterwards. Images and
# named volumes are retained, and partially running service states are not reconstructed.
# policy.py is copied into the image using COPY, so mitm is rebuilt every time, even when
# it is already running.
#
# When to run it:
#   - after changing anything under .devcontainer/
#   - after updating mitmproxy (mandatory once uv.lock has been regenerated)
#
# Some mitmproxy options have permissive defaults, and those defaults can change between
# versions. The attack tests here (Host spoofing / raw TCP / CONNECT to a non-443 port /
# path confusion / WebSocket / method override) were all added after actual security gaps
# were found. Do not delete them.
#
# How a test decides pass or fail:
#   - forwarded: X-Proxy-Deny-Reason must be empty (a proxy-generated 403 cannot be
#     distinguished from an upstream 403 by status alone)
#   - denied: the reason field must be non-empty. Attack tests may also match the reason
#     name to confirm that the intended branch was exercised, but if a reason is renamed
#     in the code it must be updated here too
# Bypass tests assert that "the peer's response never arrives", not that "nothing at all
# comes back".
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
COMPOSE="docker compose -f $REPO_ROOT/.devcontainer/compose.yaml"
CURSOR=devcontainer-cursor-1
MITM=devcontainer-mitm-1
ECHO_CONTAINER=regression-echo
EGRESS_NETWORK=devcontainer_egress
INTERNAL_NETWORK=devcontainer_internal

PASS=0
FAIL=0
STARTED_BY_US=0

pass() { echo "  ok   $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL $1"; echo "       expected: $2"; echo "       actual:   $3"; FAIL=$((FAIL + 1)); }

check() {
  local name=$1 expected=$2 actual=$3
  if [[ "$actual" == "$expected" ]]; then pass "$name"; else fail "$name" "$expected" "$actual"; fi
}

cleanup() {
  docker rm -f "$ECHO_CONTAINER" >/dev/null 2>&1
  if [[ $STARTED_BY_US -eq 1 ]]; then
    echo "==> Stopping the stack (it was down before the tests)"
    $COMPOSE down >/dev/null 2>&1
  fi
}
trap cleanup EXIT

# -----------------------------------------------------------------------------
echo "==> Preparing the stack"
# policy.py and test_policy.py are copied into the image using COPY, so always rebuild mitm.
# Running against the existing container could falsely pass with stale decision logic.
echo "    building the mitm image..."
$COMPOSE build mitm >/dev/null 2>&1 || { echo "failed to build mitm"; exit 1; }

if ! docker ps --format '{{.Names}}' | grep -q "^${CURSOR}$"; then
  STARTED_BY_US=1
  $COMPOSE up -d >/dev/null 2>&1 || { echo "failed to start the stack"; exit 1; }
else
  # Even when it is already running, replace mitm with the new image
  $COMPOSE up -d mitm >/dev/null 2>&1 || { echo "failed to restart mitm"; exit 1; }
fi

# Wait until the CA has been imported and TLS works
for _ in $(seq 1 60); do
  docker exec -u cursor "$CURSOR" curl -sS -o /dev/null --max-time 5 https://example.com >/dev/null 2>&1 && break
  sleep 1
done

# -----------------------------------------------------------------------------
echo "==> 1. Unit tests for the decision logic"
if docker exec -w /opt/proxy "$MITM" proxy-python test_policy.py > /tmp/regression-unit.log 2>&1; then
  pass "test_policy.py ($(tail -1 /tmp/regression-unit.log))"
else
  fail "test_policy.py" "all cases pass" "$(tail -3 /tmp/regression-unit.log)"
fi

# -----------------------------------------------------------------------------
echo "==> 2. Startup assertions (abort unless critical options have the expected values)"
# Pass the production values for every option except the one under test. Otherwise the
# startup aborts for a different reason and the test "passes for the wrong reason".
assert_option_aborts() {
  local name=$1 rawtcp=$2 websocket=$3
  local out
  out=$(docker exec "$MITM" mitmdump \
    --mode regular --listen-port 3129 --set "$rawtcp" --set "$websocket" \
    --set confdir=/private/ca -s /opt/proxy/policy.py 2>&1 | head -5)
  if echo "$out" | grep -q "aborting startup"; then pass "$name"; else fail "$name" "the startup abort message" "$out"; fi
}
assert_option_aborts "rawtcp=true aborts startup"    "rawtcp=true"  "websocket=false"
assert_option_aborts "websocket=true aborts startup" "rawtcp=false" "websocket=true"

# -----------------------------------------------------------------------------
echo "==> 3. Egress policy (over the network)"
probe() {
  docker exec -u cursor "$CURSOR" curl -sS -o /dev/null \
    -w '%{http_code}/%header{x-proxy-deny-reason}' --max-time 20 "$@" 2>/dev/null
}

# The HTTP status does not tell us whether the request reached upstream (401/403 come back
# because we are not authenticated). The probe output is "<status>/<deny reason>", so an
# empty reason field means the proxy let it through.
assert_forwarded() {
  local name=$1 result=$2
  if [[ "$result" == */ ]]; then
    pass "$name ($result)"
  else
    fail "$name" "not a proxy denial (empty reason field)" "$result"
  fi
}

check "GET passes on a write-disallowed domain when read=*" "200/" "$(probe https://example.com)"
check "HEAD behaves the same (read=*)"   "200/"                  "$(probe -I https://example.com)"
check "POST to a write-disallowed domain is denied" "403/default_deny" "$(probe -X POST https://example.com)"
# api2.cursor.sh may return various status codes when unauthenticated; only assert forwarding.
assert_forwarded "POST to an allowed Cursor domain is forwarded" "$(probe -X POST https://api2.cursor.sh)"
# TRACE must be checked on a bumped host: Cursor domains are TLS-passthrough by default,
# so method policy would not see the request there.
check "TRACE is denied"                  "403/method_not_allowed" "$(probe -X TRACE https://example.com)"
# The default write-allowlist restricts GitHub by path. These requests must not pass.
check "git push is denied"               "403/default_deny"      "$(probe -X POST https://github.com/o/r.git/git-receive-pack)"
check "POST to the GitHub REST API is denied" "403/default_deny" "$(probe -X POST https://api.github.com/user/repos)"
check "a GitHub path that is not allowed is denied" "403/default_deny" "$(probe -X POST https://github.com/login)"
check "path allowlist matching is case-sensitive" "403/default_deny" \
  "$(probe -X POST https://github.com/LOGIN/device/code)"
check "GraphQL without a body is denied" "403/graphql_no_body"   "$(probe -X POST https://api.github.com/graphql)"

# GraphQL: queries pass and mutations are denied (for gh auth login and read-only commands)
gql() {
  probe -X POST -H 'Content-Type: application/json' --data "$1" https://api.github.com/graphql
}
check "a GraphQL mutation is denied"     "403/graphql_mutation_denied" \
  "$(gql '{"query":"mutation { addStar(input:{starrableId:\"x\"}) { clientMutationId } }"}')"
check "malformed GraphQL is denied"      "403/graphql_parse_error" \
  "$(gql '{"query":"query { viewer {"}')"

assert_forwarded "a GraphQL query is forwarded" "$(gql '{"query":"query { viewer { login } }"}')"
# Use upload-pack on a public repository so the test does not depend on a personal
# repository name.
assert_forwarded "git fetch is forwarded" \
  "$(probe -X POST https://github.com/git/git.git/git-upload-pack)"

# -----------------------------------------------------------------------------
echo "==> 4. Attack tests"

# 4-1. Host spoofing: CONNECT to a denied destination while claiming an allowed domain in Host
check "Host spoofing is denied" "403/host_mismatch" \
  "$(probe -X POST -H 'Host: api2.cursor.sh' --data spoof https://example.com)"

# The controlled peer below is on a different bridge, which is isolated independently of
# Compose's internal flag. Assert both the current Compose model and runtime network so
# deleting `internal: true` cannot pass against a stale network created by an earlier run.
COMPOSE_INTERNAL=$($COMPOSE config 2>/dev/null \
  | awk '$1 == "internal:" && $2 == "true" { print "true"; exit }')
check "the Compose model marks the cursor network internal" "true" "$COMPOSE_INTERNAL"
INTERNAL_FLAG=$(docker network inspect -f '{{.Internal}}' "$INTERNAL_NETWORK" 2>/dev/null)
check "the runtime cursor network is marked internal" "true" "$INTERNAL_FLAG"

# Controlled peer for the direct-egress and raw-TCP bypass tests.
docker rm -f "$ECHO_CONTAINER" >/dev/null 2>&1
docker run -d --name "$ECHO_CONTAINER" --network "$EGRESS_NETWORK" --user root \
  --entrypoint python3 devcontainer-cursor:latest -c "
import socket
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('0.0.0.0', 443)); s.listen(5)
while True:
    c, _ = s.accept()
    d = c.recv(1024)
    c.sendall(b'ECHO:' + d)
    c.close()
" >/dev/null 2>&1

# Confirm first that the echo server responds. If it is not up, the bypass tests would pass
# merely because "nothing comes back" without actually verifying bypass prevention.
ECHO_READY=down
for _ in $(seq 1 10); do
  ECHO_READY=$(docker exec "$MITM" proxy-python -c "
import socket
try:
    s = socket.create_connection(('${ECHO_CONTAINER}', 443), timeout=3)
    s.sendall(b'PING')
    print('ready' if s.recv(100).startswith(b'ECHO:') else 'bad')
except Exception:
    print('down')
" 2>/dev/null)
  [[ "$ECHO_READY" == "ready" ]] && break
  sleep 1
done
check "the regression echo server responds (a prerequisite for the bypass tests below)" "ready" "$ECHO_READY"

# 4-2. Direct egress (ignoring the proxy). Connect by IP so a DNS failure cannot make the
# test pass without exercising network isolation, and require that the peer marker never
# arrives.
ECHO_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$ECHO_CONTAINER" 2>/dev/null)
if [[ -z "$ECHO_IP" ]]; then
  DIRECT_RESULT=NO_TARGET
else
  DIRECT_RESULT=$(docker exec -u cursor "$CURSOR" python3 -c "
import socket
try:
    s = socket.create_connection(('${ECHO_IP}', 443), timeout=5)
    s.sendall(b'DIRECT-EGRESS-BYPASS-TEST')
    data = s.recv(200)
    print('LEAKED' if data.startswith(b'ECHO:') else 'BLOCKED')
except Exception:
    print('BLOCKED')
" 2>/dev/null)
fi
check "a direct connection bypassing the proxy is impossible" "BLOCKED" "$DIRECT_RESULT"

# 4-3. Raw TCP: speaking something other than HTTP inside a CONNECT must not transfer data
# Detect a leak by checking whether the response came from the echo server, not merely
# whether any bytes were returned. The latter would mistake the proxy's own error page for
# a leak.
RAW_RESULT=$(docker exec -u cursor "$CURSOR" python3 -c "
import socket
try:
    s = socket.create_connection(('mitm', 3128), timeout=8)
    s.sendall(b'CONNECT ${ECHO_CONTAINER}:443 HTTP/1.1\r\nHost: ${ECHO_CONTAINER}:443\r\n\r\n')
    s.recv(200)
    s.sendall(b'RAW-TCP-BYPASS-TEST')
    data = s.recv(200)
    print('LEAKED' if data.startswith(b'ECHO:') else 'BLOCKED')
except Exception:
    print('BLOCKED')
" 2>/dev/null)
check "a raw TCP bypass is impossible" "BLOCKED" "$RAW_RESULT"

# 4-4. CONNECT to a port other than 443
CONNECT_RESULT=$(docker exec -u cursor "$CURSOR" python3 -c "
import socket
try:
    s = socket.create_connection(('mitm', 3128), timeout=8)
    s.sendall(b'CONNECT ${ECHO_CONTAINER}:22 HTTP/1.1\r\nHost: ${ECHO_CONTAINER}:22\r\n\r\n')
    print(s.recv(200).split(b'\r\n')[0].decode().split(' ')[1])
except Exception as exc:
    print('ERROR')
" 2>/dev/null)
check "CONNECT to a port other than 443 is denied" "403" "$CONNECT_RESULT"

# 4-5. Path confusion: bypassing the path restriction with a representation the origin
#      reinterprets. curl collapses dot segments by default, so --path-as-is is required.
check "traversal cannot bypass the GraphQL inspection" "403/unsafe_path" \
  "$(probe --path-as-is -X POST https://api.github.com/login/../graphql \
      -H 'Content-Type: application/json' --data '{"query":"mutation { __typename }"}')"
check "git push through traversal is denied" "403/unsafe_path" \
  "$(probe --path-as-is -X POST https://github.com/login/../o/r.git/git-receive-pack)"
check "encoded dots are denied too" "403/unsafe_path" \
  "$(probe --path-as-is -X POST https://github.com/login/%2e%2e/user/repos)"
check "git push through traversal with %2f is denied" "403/unsafe_path" \
  "$(probe --path-as-is -X POST https://github.com/login/..%2fo/r.git/git-receive-pack)"
check "traversal with %2f cannot bypass the GraphQL inspection" "403/unsafe_path" \
  "$(probe --path-as-is -X POST https://api.github.com/login/..%2fgraphql \
      -H 'Content-Type: application/json' --data '{"query":"mutation { __typename }"}')"
check "/login on api.github.com is not allowed" "403/default_deny" \
  "$(probe -X POST https://api.github.com/login/device/code)"
# curl drops the # in a URL as a fragment, so check it with a raw request.
FRAGMENT_RESULT=$(docker exec -u cursor "$CURSOR" python3 -c "
import socket
s = socket.create_connection(('mitm', 3128), timeout=8)
s.sendall(b'POST http://github.com/o/r.git/git-receive-pack#/git-upload-pack HTTP/1.1\r\n'
          b'Host: github.com\r\nContent-Length: 0\r\n\r\n')
head = s.recv(400).decode('latin1').split('\r\n\r\n')[0]
print(next((l.split(': ', 1)[1] for l in head.split('\r\n')
            if l.lower().startswith('x-proxy-deny-reason')), head.split('\r\n')[0]))
" 2>/dev/null)
check "faking a suffix match with # is denied" "unsafe_path" "$FRAGMENT_RESULT"

# 4-6. WebSocket to a read-allowed destination (everything after the 101 bypasses request(),
#      and this addon has no WebSocket message hook, which would make it a full-duplex hole)
#      The WebSocket handshake is HTTP/1.1. Over HTTP/2 the Upgrade header is dropped as a
#      connection-specific header, so --http1.1 is required for the request to be one we
#      actually inspect.
check "an upgrade to WebSocket is denied" "403/websocket_not_allowed" \
  "$(probe --http1.1 -H 'Connection: Upgrade' -H 'Upgrade: websocket' https://example.com/)"

# 4-7. Disguising a read as a write with a method-override header or query parameter
check "a GET with a method override is judged as a write" "403/method_override_denied" \
  "$(probe -H 'X-HTTP-Method-Override: POST' https://example.com/)"
check "the _method query parameter is judged as a write too" "403/method_override_denied" \
  "$(probe 'https://example.com/?_method=POST')"
check "an encoded _method query key is judged as a write too" "403/method_override_denied" \
  "$(probe 'https://example.com/?%5fmethod=POST')"
check "a semicolon-separated _method query key is judged as a write too" "403/method_override_denied" \
  "$(probe 'https://example.com/?x=1;_method=POST')"
check "GraphQL cannot reclassify an overridden GET as a read" "403/method_override_denied" \
  "$(probe -X GET -H 'X-HTTP-Method-Override: POST' -H 'Content-Type: application/json' \
      --data '{"query":"query { viewer { login } }"}' https://api.github.com/graphql)"

# -----------------------------------------------------------------------------
echo "==> 5. Loading the allowlists"

# read-allowlist defaults to "*" (every domain allowed). Check the actual value in the
# startup log, not merely that some read allowlist was loaded.
READ_LOG=$(docker logs "$MITM" 2>&1 | grep '\[policy\] ok: read=' | tail -1)
if [[ "$READ_LOG" == *"read=* (all domains allowed;"* ]]; then
  pass "the allowlists are loaded (${READ_LOG#*ok: })"
else
  fail "the default read allowlist is loaded" "read=* in the startup log" "${READ_LOG:-(no output)}"
fi

# An invalid configuration must abort startup (preventing writes from being opened up or
# an entry from becoming ambiguous)
assert_startup_aborts() {
  local name=$1 var=$2 content=$3
  local out
  out=$(docker exec "$MITM" sh -c "
    printf '%s\n' '$content' > /tmp/bad-allowlist.txt
    env $var=/tmp/bad-allowlist.txt mitmdump --mode regular --listen-port 3129 \
      --set rawtcp=false --set websocket=false --set confdir=/private/ca \
      -s /opt/proxy/policy.py 2>&1 | head -6
    rm -f /tmp/bad-allowlist.txt" 2>&1)
  if echo "$out" | grep -q "aborting startup"; then pass "$name"; else fail "$name" "startup aborts" "$out"; fi
}
assert_startup_aborts '"*" in write-allowlist aborts startup' WRITE_ALLOWLIST '*'
assert_startup_aborts 'a * in the middle of a path aborts startup' WRITE_ALLOWLIST '.example.com  /a/*/b'
# A typo that "simply never matches" goes unnoticed, so fail at startup to surface it.
assert_startup_aborts 'a malformed domain aborts startup' WRITE_ALLOWLIST '*.example.com'
assert_startup_aborts '"*" in graphql-endpoints aborts startup' GRAPHQL_ENDPOINTS '*'
assert_startup_aborts '"*" in tls-passthrough aborts startup' TLS_PASSTHROUGH '*'
assert_startup_aborts 'a path pattern in tls-passthrough aborts startup' TLS_PASSTHROUGH '.cursor.sh  /api/*'

# -----------------------------------------------------------------------------
echo "==> 5b. TLS passthrough vs SSL Bump (certificate issuer)"

# Passthrough hosts present the real upstream certificate. Bumped hosts present the
# DevContainer Proxy CA. curl -v prints "issuer: ..." on stderr.
issuer_line() {
  docker exec -u cursor "$CURSOR" curl -sS -o /dev/null -v --max-time 20 "$1" 2>&1 \
    | grep -i 'issuer:' | head -1
}

CURSOR_ISSUER=$(issuer_line https://api2.cursor.sh || true)
if [[ -n "$CURSOR_ISSUER" && "$CURSOR_ISSUER" != *"DevContainer Proxy"* ]]; then
  pass "api2.cursor.sh is TLS-passthrough (issuer is not the proxy CA)"
else
  fail "api2.cursor.sh is TLS-passthrough" "upstream issuer without DevContainer Proxy" \
    "${CURSOR_ISSUER:-(no issuer line)}"
fi

BUMP_ISSUER=$(issuer_line https://example.com || true)
if [[ "$BUMP_ISSUER" == *"DevContainer Proxy"* ]]; then
  pass "example.com is still SSL-bumped (proxy CA issuer)"
else
  fail "example.com is still SSL-bumped" "issuer containing DevContainer Proxy" \
    "${BUMP_ISSUER:-(no issuer line)}"
fi

PASSTHROUGH_LOG=$(docker logs "$MITM" 2>&1 | grep 'PASSTHROUGH api2.cursor.sh' | tail -1)
if [[ -n "$PASSTHROUGH_LOG" ]]; then
  pass "passthrough decisions are logged (${PASSTHROUGH_LOG})"
else
  fail "passthrough decisions are logged" "PASSTHROUGH api2.cursor.sh in mitm logs" "(none)"
fi

# -----------------------------------------------------------------------------
echo "==> 6. CA placement (no private key on the cursor side)"
check "the CA private key is not visible to cursor" "absent" \
  "$(docker exec -u cursor "$CURSOR" sh -c 'ls /private/ca >/dev/null 2>&1 && echo present || echo absent')"
check "the public certificate is available to cursor" "present" \
  "$(docker exec -u cursor "$CURSOR" sh -c 'test -f /shared/ca/proxy-ca.crt && echo present || echo absent')"
check "the private key is on the mitm side" "present" \
  "$(docker exec "$MITM" sh -c 'test -f /private/ca/mitmproxy-ca.pem && echo present || echo absent')"
check ".devcontainer is read-only from inside the container" "readonly" \
  "$(docker exec -u cursor "$CURSOR" sh -c 'touch /workspace/.devcontainer/.wtest 2>/dev/null && rm -f /workspace/.devcontainer/.wtest && echo writable || echo readonly')"

# -----------------------------------------------------------------------------
echo
echo "================================================"
echo "  passed: $PASS / failed: $FAIL"
echo "================================================"
[[ $FAIL -eq 0 ]] || exit 1
