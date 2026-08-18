"""Verify the decisions made by policy.py.

This module is run from .devcontainer/tests/regression-suite.sh.

Boundary cases that are hard to exercise over the network (path prefix/suffix matching,
apex matching, IPv6, GraphQL evasion tricks, fail-closed behavior) are covered here.
"""

import json
import os
import sys
import tempfile

import policy
from policy import decide, host_mismatch, parse_entry

# The tests do not depend on the configuration files, so they keep working when those
# files change. WRITE contains the same entries as the default write-allowlist.txt.
WRITE = tuple(
    parse_entry(line)
    for line in (
        ".cursor.sh",
        ".cursorapi.com",
        ".cursor-cdn.com",
        ".cursorvm.com",
        "marketplace.visualstudio.com",
        "marketplace.cursorapi.com",
        "github.com  /login/*",
        ".github.com  */git-upload-pack",
    )
)
READ_ANY = (parse_entry("*", allow_wildcard=True),)
READ_LIMITED = tuple(parse_entry(line) for line in (".cursor.sh", "example.com"))
GRAPHQL = (parse_entry("api.github.com  /graphql"),)

# (method, host, path, expected_allowed, note)
CASES = [
    # --- reads: the default read-allowlist ("*") allows every domain ---
    ("GET", "example.com", "/", True, "GET passes on a write-disallowed domain when read=*"),
    ("HEAD", "example.com", "/", True, "HEAD behaves the same (read=*)"),
    ("GET", "api.github.com", "/graphql", True, "GET passes on GitHub too"),
    ("get", "example.com", "/", True, "the method is case-insensitive"),

    # --- writes: domain-only entries ---
    ("POST", "api2.cursor.sh", "/", True, "allowed domain"),
    ("POST", "cursor.sh", "/", True, "a leading dot matches the apex too"),
    ("POST", "assets.cursor-cdn.com", "/x", True, "subdomain"),
    ("PUT", "api2.cursor.sh", "/x", True, "PUT is a write method as well"),
    ("POST", "marketplace.visualstudio.com", "/_apis/x", True, "no dot = exact match"),
    ("POST", "evil.marketplace.visualstudio.com", "/x", False, "exact match excludes subdomains"),
    ("POST", "example.com", "/", False, "write to a domain that is not allowed"),
    ("DELETE", "example.com", "/", False, "DELETE behaves the same"),
    ("POST", "notcursor.sh", "/", False, "a partial suffix match must not allow it"),

    # --- methods that are never allowed ---
    ("TRACE", "api2.cursor.sh", "/", False, "TRACE is denied even on an allowed domain"),
    ("CONNECT", "api2.cursor.sh", "/", False,
     "denied even though CONNECT is not expected after decryption"),

    # --- writes: entries with a path pattern (GitHub) ---
    ("POST", "github.com", "/login/device/code", True, "prefix match /login/*"),
    ("POST", "github.com", "/LOGIN/device/code", False,
     "path matching is case-sensitive, as it is at the origin"),
    ("POST", "github.com", "/login/oauth/access_token", True, "same as above"),
    ("POST", "github.com", "/login", False, "/login/* requires the trailing slash"),
    ("POST", "github.com", "/x/login/y", False, "a prefix match cannot match in the middle"),
    ("POST", "github.com", "/o/r.git/git-upload-pack", True, "suffix match */git-upload-pack"),
    ("POST", "github.com", "/o/r.git/git-upload-pack?x=1", True,
     "the query string is ignored during matching"),
    ("POST", "github.com", "/o/r.git/git-receive-pack", False, "git push is denied"),
    ("POST", "api.github.com", "/user/repos", False, "writes through the GitHub REST API are denied"),
    ("POST", "raw.githubusercontent.com", "/x", False, "domain that is not in the allowlist"),
    ("TRACE", "github.com", "/login/x", False, "TRACE is denied on GitHub too"),

    # --- path representations that the proxy and the origin interpret differently ---
    ("POST", "api.github.com", "/login/../graphql", False,
     "traversal must not bypass the GraphQL inspection"),
    ("POST", "github.com", "/login/../o/r.git/git-receive-pack", False,
     "traversal must not let git push through"),
    ("POST", "github.com", "/login/%2e%2e/user/repos", False, "encoded dots are denied too"),
    ("POST", "github.com", "/login/%252e%252e/user/repos", False, "double encoding is denied too"),
    ("POST", "github.com", "/login/..%2fo/r.git/git-receive-pack", False,
     "traversal with %2f must not let git push through"),
    ("POST", "github.com", "/login/foo%2f..%2f..%2fo/r.git/git-receive-pack", False,
     "%2f.. inside a segment is denied as well"),
    ("POST", "api.github.com", "/login/..%2fgraphql", False,
     "%2f must not bypass the GraphQL inspection"),
    ("POST", "api.github.com", "/login/device/code", False,
     "/login/* applies to the github.com apex only (not to the API subdomain)"),
    ("POST", "github.com", "/o/r.git/git-receive-pack#/git-upload-pack", False,
     "a suffix match must not be faked after # (matching only strips the query)"),
    ("POST", "github.com", "/o/r.git/git-receive-pack;/git-upload-pack", False,
     "guards against origins that truncate at path parameters"),
    ("POST", "github.com", "/login/./device/code", False,
     "even a harmless-looking . segment is rejected"),
    ("GET", "example.com", "/a/../b", False, "reads are treated the same way"),
    ("GET", "api.github.com", "/repos/o/r/branches/feature%2Fbar", True,
     "%2f is allowed since gh uses it legitimately in branch names (no dot segment)"),
    ("GET", "example.com", "/?a=/../b", True,
     "the query does not affect routing, so it is out of scope"),
    ("POST", "github.com", "/login/%5c../user/repos", False, "an encoded backslash is denied too"),

    # --- ports ---
    ("GET", "example.com", "/", False, "outside Safe_ports", 8080),
    ("GET", "example.com", "/", True, "80 is allowed", 80),
]

# Requests carrying a method-override header or query parameter. On an origin that honors
# either form, a read becomes a write.
# (method, host, path, expected_allowed, note, header_override[, body])
# Cases with header_override=False rely on decide() detecting _method= in the path.
OVERRIDE_CASES = [
    ("GET", "example.com", "/", False,
     "an override makes it a write even on a read-allowed destination", True),
    ("HEAD", "example.com", "/", False, "HEAD behaves the same", True),
    ("GET", "api2.cursor.sh", "/aiserver", True, "passes on a write-allowed destination", True),
    ("POST", "api2.cursor.sh", "/aiserver", True, "requests that were already writes are unaffected", True),
    ("GET", "example.com", "/x?_method=POST", False,
     "the _method query parameter is treated as a write too", False),
    ("GET", "example.com", "/x?%5fmethod=POST", False,
     "a URL-encoded _method key is treated as a write", False),
    ("GET", "example.com", "/x?%255fmethod=POST", False,
     "a nested URL-encoded _method key is treated as a write", False),
    ("GET", "example.com", "/x?a=1;_method=POST", False,
     "a semicolon-separated _method key is treated as a write", False),
    ("GET", "example.com", "/x?a=1%26%5fmethod%3dPOST", False,
     "encoded separators cannot disguise a _method key", False),
    ("GET", "api2.cursor.sh", "/aiserver?_method=PUT", True,
     "_method also passes on a write-allowed destination", False),
    ("GET", "api.github.com", "/graphql", False,
     "GraphQL classification cannot turn an overridden read back into a query", True,
     b'{"query":"query { viewer { login } }"}'),
]

# Behavior with a narrowed read-allowlist. (method, host, path, expected_allowed, note)
READ_LIMITED_CASES = [
    ("GET", "api2.cursor.sh", "/", True, "a subdomain that is on the list"),
    ("GET", "cursor.sh", "/", True, "a leading dot matches the apex too"),
    ("GET", "example.com", "/", True, "no dot means an exact match"),
    ("HEAD", "example.com", "/", True, "HEAD is treated the same"),
    ("GET", "sub.example.com", "/", False, "no dot, so subdomains are excluded"),
    ("GET", "evil.com", "/", False, "anything off the list is denied"),
    ("GET", "notcursor.sh", "/", False, "a partial suffix match must not allow it"),
    ("GET", "github.com", "/", False, "once narrowed, GitHub needs an explicit entry too"),
]


def _gql(query, **extra):
    payload = {"query": query}
    payload.update(extra)
    return json.dumps(payload).encode()


# (body, expected_allowed, note) - read is "*", and api.github.com is not write-allowed
GRAPHQL_CASES = [
    (_gql("query { viewer { login } }"), True, "the query gh auth login uses in its final step"),
    (_gql("{ viewer { login } }"), True, "anonymous query"),
    (_gql('query PRs { repository(owner:"o", name:"r") { pullRequests(first:10) { nodes { title } } } }'),
     True, "the query behind gh pr list"),
    (_gql("query A { viewer { login } }\nquery B { viewer { id } }"), True, "multiple queries"),
    (_gql('query { search(query: "mutation is a word", type: ISSUE, first: 1) { issueCount } }'),
     True, 'the word "mutation" inside a string literal is not a false positive'),

    (_gql('mutation { addStar(input:{starrableId:"x"}) { clientMutationId } }'),
     False, "the mutation is denied because the endpoint is not write-allowed"),
    (_gql("query A { viewer { login } }\nmutation B { addStar(input:{}) { clientMutationId } }"),
     False, "a document containing both a query and a mutation counts as a write"),
    (_gql("query A { viewer { login } }\nmutation B { addStar(input:{}) { clientMutationId } }",
          operationName="A"),
     False, "selecting the query with operationName does not let the document through"),
    (_gql("subscription { x }"), False, "a subscription counts as a write too"),
    (_gql("MUTATION { addStar(input:{}) { clientMutationId } }"), False,
     "an uppercase operation keyword is denied as a syntax error"),
    (_gql("query { viewer { login }"), False, "a malformed query is denied"),
    (b"", False, "an empty body is denied"),
    (b"not json", False, "a body that is not JSON is denied"),
    (b'{"variables": {}}', False, 'denied when the "query" key is missing'),
    (b"[]", False, "an empty batch is denied"),
    (b'[{"query": "query { viewer { login } }"}]', True,
     "a batch containing only queries is allowed"),
    (b'[{"query": "query { viewer { login } }"}, {"query": "mutation { addStar(input:{}) { clientMutationId } }"}]',
     False, "a batch containing a mutation counts as a write"),
    (b'{"query": 123}', False, 'denied when "query" is not a string'),
    (_gql("query { viewer { login } }" + " " * (256 * 1024)), False, "an oversized body is denied"),
]

# (conn_host, host_header, expected_mismatch, note)
HOST_CASES = [
    ("example.com", "example.com", False, "match"),
    ("example.com", "example.com:443", False, "matches with a port too"),
    ("example.com", "EXAMPLE.COM", False, "case-insensitive"),
    ("example.com", "example.com.", False, "a trailing dot is ignored"),
    ("example.com", "api2.cursor.sh", True,
     "spoof: CONNECT to a denied destination while claiming an allowed one"),
    ("example.com", "", False, "a missing Host header is not a mismatch"),
    ("api2.cursor.sh", "cursor.sh", True, "the parent domain is a mismatch too"),
    ("::1", "[::1]:3128", False, "IPv6 literal"),
]

# Invalid configuration lines are rejected at startup. (line, allow_wildcard, note)
# Letting them "just not match" would mean operating without knowing why something is
# denied.
BAD_ENTRY_CASES = [
    ("*", False, "* is rejected in write-allowlist / graphql-endpoints"),
    (".example.com  /a/*/b", False, "* in a path is allowed only at the start or end"),
    (".example.com  api/*", False, "a path must start with / or *"),
    (".example.com  /a  /b", False, "too many fields"),
    ("*.github.com", False, "mistaken use of * instead of the leading-dot notation"),
    ("https://github.com", False, "a scheme is rejected"),
    ("github.com/login", False,
     "the domain and path must be separated by whitespace, not joined with a slash"),
    (".example.com  **", False, "at most one *"),
    (".example.com  */foo*", False, "it cannot be placed at both the start and the end"),
]

PASSTHROUGH = tuple(
    (parse_entry(line)[0], None)
    for line in (".cursor.sh", ".cursorapi.com", ".cursor-cdn.com", ".cursorvm.com")
)


class _StubHeaders(dict):
    """Case-insensitive, matching mitmproxy's Headers."""

    def get(self, key, default=""):
        for name, value in self.items():
            if name.lower() == key.lower():
                return value
        return default


class _StubRequest:
    def __init__(self, method="POST", host="example.com", path="/", port=443, host_header=None,
                 headers=None):
        self.method = method
        self.host = host
        self.path = path
        self.port = port
        self.host_header = host_header
        self.headers = _StubHeaders(headers or {})
        self.stream = False

    def get_content(self, strict=True):
        return b""


class _StubResponse:
    def __init__(self):
        self.stream = False
        self.headers = _StubHeaders()
        self.status_code = 200


class _StubConn:
    def __init__(self, address):
        self.address = address


class _StubFlow:
    def __init__(self, request, address, response=None):
        self.request = request
        self.server_conn = _StubConn(address)
        self.response = response

    def kill(self):
        self.response = "killed"


def run_flow_cases():
    """Verify the behavior of the request() hook (spoof blocking, fail-closed, healthcheck).

    Returns (failures, checks performed).
    """
    failures = 0
    checks = 0

    # request() uses the configuration loaded at startup (module variables). The unit
    # tests never go through running(), so reproduce the post-startup state here.
    saved = (policy.READ_ALLOWED, policy.WRITE_ALLOWED, policy.GRAPHQL_ENDPOINTS)
    policy.READ_ALLOWED, policy.WRITE_ALLOWED, policy.GRAPHQL_ENDPOINTS = READ_ANY, WRITE, GRAPHQL

    def check(name, flow, expected_status, expected_reason):
        nonlocal failures, checks
        checks += 1
        policy.request(flow)
        response = flow.response
        status = getattr(response, "status_code", None)
        reason = response.headers.get("X-Proxy-Deny-Reason", "") if response is not None else ""
        ok = status == expected_status and reason == expected_reason
        if not ok:
            failures += 1
        mark = "ok  " if ok else "FAIL"
        print(mark + " " + name + ": status=" + str(status) + " reason=" + repr(reason))

    spoof = _StubFlow(
        _StubRequest(host="example.com", host_header="api2.cursor.sh"),
        ("example.com", 443),
    )
    check("Host spoofing is denied", spoof, 403, "host_mismatch")

    allowed_flow = _StubFlow(
        _StubRequest(host="api2.cursor.sh", host_header="api2.cursor.sh"),
        ("api2.cursor.sh", 443),
    )
    policy.request(allowed_flow)
    passed = allowed_flow.response is None
    checks += 1
    if not passed:
        failures += 1
    print(("ok  " if passed else "FAIL") + " POST to an allowed domain is forwarded")

    # WebSocket frames after the 101 do not go through request(), and this addon has no
    # WebSocket message hook, so stop the handshake.
    upgrade = _StubFlow(
        _StubRequest(method="GET", host="example.com", path="/ws", host_header="example.com",
                     headers={"Upgrade": "WebSocket", "Connection": "Upgrade"}),
        ("example.com", 443),
    )
    check("an upgrade to WebSocket is denied", upgrade, 403, "websocket_not_allowed")

    # For plaintext HTTP there is no address yet, so it falls back to request.host (safe,
    # since that is the same value that will be connected to).
    plain_http = _StubFlow(
        _StubRequest(host="api2.cursor.sh", host_header="api2.cursor.sh", port=80), None
    )
    policy.request(plain_http)
    passed = plain_http.response is None
    checks += 1
    if not passed:
        failures += 1
    print(("ok  " if passed else "FAIL")
          + " before a plaintext HTTP connection exists, the decision uses request.host")

    # When neither is available, do not decide without knowing the destination.
    no_host = _StubFlow(_StubRequest(host=None, host_header="api2.cursor.sh"), None)
    check("denied when the destination cannot be determined", no_host, 403, "missing_server_address")

    override = _StubFlow(
        _StubRequest(method="GET", host="example.com", host_header="example.com",
                     headers={"x-http-method-override": "POST"}),
        ("example.com", 443),
    )
    check("a GET with a method override is judged as a write", override, 403,
          "method_override_denied")

    query_override = _StubFlow(
        _StubRequest(method="GET", host="example.com", path="/x?_method=POST",
                     host_header="example.com"),
        ("example.com", 443),
    )
    check("the _method query parameter is judged as a write too", query_override, 403,
          "method_override_denied")

    original = policy.decide

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    policy.decide = boom
    try:
        broken = _StubFlow(
            _StubRequest(host="api2.cursor.sh", host_header="api2.cursor.sh"),
            ("api2.cursor.sh", 443),
        )
        check("an exception during decision-making causes denial (fail-closed)",
              broken, 403, "policy_error")
    finally:
        policy.decide = original

    health = _StubFlow(
        _StubRequest(method="GET", host="localhost", port=3128, host_header="localhost:3128"),
        ("localhost", 3128),
    )
    check("the healthcheck returns 200", health, 200, "")

    policy.READ_ALLOWED, policy.WRITE_ALLOWED, policy.GRAPHQL_ENDPOINTS = saved
    return failures, checks


def run_stream_cases():
    """Verify requestheaders / responseheaders enable streaming except for GraphQL."""
    failures = 0
    checks = 0

    saved = (policy.READ_ALLOWED, policy.WRITE_ALLOWED, policy.GRAPHQL_ENDPOINTS)
    policy.READ_ALLOWED, policy.WRITE_ALLOWED, policy.GRAPHQL_ENDPOINTS = READ_ANY, WRITE, GRAPHQL

    def check(name, ok):
        nonlocal failures, checks
        checks += 1
        if not ok:
            failures += 1
        print(("ok  " if ok else "FAIL") + " " + name)

    cursor = _StubFlow(
        _StubRequest(host="api2.cursor.sh", path="/aiserver.v1.HealthService/StreamSSE",
                     host_header="api2.cursor.sh"),
        ("api2.cursor.sh", 443),
    )
    policy.requestheaders(cursor)
    check("requestheaders streams an allowed bumped-host POST",
          cursor.request.stream is True and cursor.response is None)

    graphql = _StubFlow(
        _StubRequest(host="api.github.com", path="/graphql", host_header="api.github.com"),
        ("api.github.com", 443),
    )
    policy.requestheaders(graphql)
    check("requestheaders keeps buffering for GraphQL body inspection",
          graphql.request.stream is False and graphql.response is None)

    unknown = _StubFlow(_StubRequest(host=None, host_header="api2.cursor.sh"), None)
    policy.requestheaders(unknown)
    check("requestheaders keeps buffering when the destination is unknown",
          unknown.request.stream is False and unknown.response is None)

    spoof = _StubFlow(
        _StubRequest(host="example.com", host_header="api2.cursor.sh"),
        ("example.com", 443),
    )
    policy.requestheaders(spoof)
    spoof_reason = spoof.response.headers.get("X-Proxy-Deny-Reason", "") if spoof.response else ""
    check("requestheaders denies Host spoofing before streaming",
          spoof.request.stream is False and spoof_reason == "host_mismatch")

    unsafe = _StubFlow(
        _StubRequest(host="api.github.com", path="/login/../graphql",
                     host_header="api.github.com"),
        ("api.github.com", 443),
    )
    policy.requestheaders(unsafe)
    unsafe_reason = unsafe.response.headers.get("X-Proxy-Deny-Reason", "") if unsafe.response else ""
    check("requestheaders denies unsafe paths before streaming",
          unsafe.request.stream is False and unsafe_reason == "unsafe_path")

    streamed = _StubFlow(
        _StubRequest(host="api2.cursor.sh", host_header="api2.cursor.sh"),
        ("api2.cursor.sh", 443),
        response=_StubResponse(),
    )
    policy.responseheaders(streamed)
    check("responseheaders streams every response", streamed.response.stream is True)

    policy.READ_ALLOWED, policy.WRITE_ALLOWED, policy.GRAPHQL_ENDPOINTS = saved
    return failures, checks


class _StubClientHello:
    def __init__(self, sni=None):
        self.sni = sni


class _StubServer:
    def __init__(self, address):
        self.address = address


class _StubContext:
    def __init__(self, address):
        self.server = _StubServer(address)


class _StubClientHelloData:
    def __init__(self, address, sni=None):
        self.context = _StubContext(address)
        self.client_hello = _StubClientHello(sni)
        self.ignore_connection = False


def run_passthrough_cases():
    """Verify tls_clienthello passthrough matching and fail-closed behavior."""
    failures = 0
    checks = 0

    saved = policy.TLS_PASSTHROUGH
    policy.TLS_PASSTHROUGH = PASSTHROUGH

    def check(name, ok):
        nonlocal failures, checks
        checks += 1
        if not ok:
            failures += 1
        print(("ok  " if ok else "FAIL") + " " + name)

    cursor = _StubClientHelloData(("api2.cursor.sh", 443), sni="api2.cursor.sh")
    policy.tls_clienthello(cursor)
    check("tls_clienthello passes through Cursor Agent hosts",
          cursor.ignore_connection is True)

    agent = _StubClientHelloData(("agentn.global.api5.cursor.sh", 443),
                                 sni="agentn.global.api5.cursor.sh")
    policy.tls_clienthello(agent)
    check("tls_clienthello passes through agentn.global.api5.cursor.sh",
          agent.ignore_connection is True)

    other = _StubClientHelloData(("example.com", 443), sni="example.com")
    policy.tls_clienthello(other)
    check("tls_clienthello keeps intercepting non-passthrough hosts",
          other.ignore_connection is False)

    mismatch = _StubClientHelloData(("example.com", 443), sni="api2.cursor.sh")
    policy.tls_clienthello(mismatch)
    check("tls_clienthello does not passthrough on CONNECT/SNI mismatch",
          mismatch.ignore_connection is False)

    missing = _StubClientHelloData(None, sni="api2.cursor.sh")
    policy.tls_clienthello(missing)
    check("tls_clienthello keeps intercepting when the CONNECT target is missing",
          missing.ignore_connection is False)

    check("match_passthrough_host accepts a Cursor subdomain",
          policy.match_passthrough_host("api2.cursor.sh", PASSTHROUGH))
    check("match_passthrough_host rejects an unrelated host",
          not policy.match_passthrough_host("example.com", PASSTHROUGH))

    with tempfile.TemporaryDirectory() as tmp:
        bad_star = os.path.join(tmp, "star.txt")
        with open(bad_star, "w") as handle:
            handle.write("*\n")
        try:
            policy.load_passthrough(bad_star)
            check("load_passthrough rejects *", False)
        except ValueError:
            check("load_passthrough rejects *", True)

        bad_path = os.path.join(tmp, "path.txt")
        with open(bad_path, "w") as handle:
            handle.write(".cursor.sh  /api/*\n")
        try:
            policy.load_passthrough(bad_path)
            check("load_passthrough rejects path patterns", False)
        except ValueError:
            check("load_passthrough rejects path patterns", True)

    policy.TLS_PASSTHROUGH = saved
    return failures, checks


def main():
    failures = 0

    for case in CASES:
        method, host, path, expected, note = case[:5]
        port = case[5] if len(case) > 5 else 443
        allowed, reason = decide(method, host, path, port, READ_ANY, WRITE, GRAPHQL)
        ok = allowed == expected
        if not ok:
            failures += 1
        mark = "ok  " if ok else "FAIL"
        verdict = "allow" if allowed else "deny "
        print(mark + " " + verdict + " (" + reason.ljust(22) + ") " + method.ljust(7) + " " + host + path + "  -- " + note)

    print()
    for case in OVERRIDE_CASES:
        method, host, path, expected, note, header_override = case[:6]
        body = case[6] if len(case) > 6 else b""
        allowed, reason = decide(method, host, path, 443, READ_ANY, WRITE, GRAPHQL,
                                 body=body, method_override=header_override)
        ok = allowed == expected
        if not ok:
            failures += 1
        mark = "ok  " if ok else "FAIL"
        verdict = "allow" if allowed else "deny "
        print(mark + " " + verdict + " (" + reason.ljust(22) + ") override+" + method.ljust(5)
              + " " + host + path + "  -- " + note)

    print()
    for method, host, path, expected, note in READ_LIMITED_CASES:
        allowed, reason = decide(method, host, path, 443, READ_LIMITED, WRITE, GRAPHQL)
        ok = allowed == expected
        if not ok:
            failures += 1
        mark = "ok  " if ok else "FAIL"
        verdict = "allow" if allowed else "deny "
        print(mark + " " + verdict + " (" + reason.ljust(22) + ") " + method.ljust(5) + " " + host.ljust(20) + " -- " + note)

    print()
    for body, expected, note in GRAPHQL_CASES:
        allowed, reason = decide("POST", "api.github.com", "/graphql", 443,
                                 READ_ANY, WRITE, GRAPHQL, body)
        ok = allowed == expected
        if not ok:
            failures += 1
        mark = "ok  " if ok else "FAIL"
        verdict = "allow" if allowed else "deny "
        print(mark + " " + verdict + " (" + reason.ljust(24) + ") -- " + note)

    print()
    for conn, header, expected, note in HOST_CASES:
        got = host_mismatch(conn, header)
        ok = got == expected
        if not ok:
            failures += 1
        mark = "ok  " if ok else "FAIL"
        verdict = "mismatch" if got else "match   "
        print(mark + " " + verdict + " conn=" + conn.ljust(18) + " host_header=" + repr(header).ljust(22) + " -- " + note)

    print()
    for line, allow_wildcard, note in BAD_ENTRY_CASES:
        try:
            parse_entry(line, allow_wildcard)
            failures += 1
            print("FAIL accepted unexpectedly: " + repr(line) + "  -- " + note)
        except ValueError:
            print("ok   rejected " + repr(line).ljust(28) + " -- " + note)

    print()
    flow_failures, flow_checks = run_flow_cases()
    failures += flow_failures

    print()
    stream_failures, stream_checks = run_stream_cases()
    failures += stream_failures

    print()
    passthrough_failures, passthrough_checks = run_passthrough_cases()
    failures += passthrough_failures

    total = (len(CASES) + len(OVERRIDE_CASES) + len(READ_LIMITED_CASES) + len(GRAPHQL_CASES)
             + len(HOST_CASES) + len(BAD_ENTRY_CASES) + flow_checks + stream_checks
             + passthrough_checks)
    print()
    print(str(total - failures) + "/" + str(total) + " passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
