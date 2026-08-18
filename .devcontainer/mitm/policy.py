"""Egress policy for the cursor container (mitmproxy addon).

Order and meaning of the decisions:

    1. Deny ports outside SAFE_PORTS (80/443)
    2. Deny path representations the origin could reinterpret (.. / %2e / # etc.)
    3. Treat a read carrying a method override (header or the _method= query
         parameter) as a write
    4. GET/HEAD           -> allow if it matches read-allowlist.txt
    5. Deny methods that are neither read nor write (TRACE etc.)
    6. POST to a GraphQL endpoint (graphql-endpoints.txt)
         -> parse the body; a query may be allowed by either allowlist,
            while any non-query operation is evaluated as a write
    7. Write methods      -> allow if they match write-allowlist.txt
    8. Deny by default

Upgrades to WebSocket are denied (even though the handshake is a GET). Frames after
the 101 do not go through the HTTP request hook, and this addon defines no WebSocket
message hook, so allowing one would open a full-duplex channel to any read-allowed
destination.

Allowlist entries are written as "domain + optional path pattern". Cases such as GitHub
authentication or git fetch, where only specific paths are permitted, can all be
expressed in configuration, so the code carries no domain-specific branches.

Design points (each was added after an actual security gap was found):

  * Always decide against the destination actually being connected to. mitmproxy's
    request.pretty_host prefers the Host header, so using it lets Host spoofing pass
    the allowlist check.
  * Deny requests whose Host header disagrees with the connection target (the
    equivalent of squid's certificate pinning).
  * Do not normalize the path; accept only representations that the origin cannot
    reinterpret. The raw path is what gets forwarded, so normalizing on the proxy side
    would not remove the interpretation gap with the origin.
  * Always fall back to denying when an exception occurs (fail-closed).
  * Abort startup if a critical option (rawtcp etc.) does not have the expected value.
    With rawtcp's permissive default, non-HTTP traffic inside a CONNECT passes straight
    through without reaching the HTTP request hook; this addon defines no TCP message hook.
  * Stream request and response bodies on SSL-bumped connections. mitmproxy buffers both
    sides unless stream is set in the *headers hooks; that buffering breaks long-lived
    streams (SSE / Connect-style) on bumped hosts. Cursor Agent traffic normally uses
    tls-passthrough.txt instead (end-to-end TLS); streaming here is for everything that
    remains intercepted. Request streaming is enabled only after a header-only allow
    decision; GraphQL endpoints keep request buffering so decide() can still parse the
    body. Denies happen in requestheaders before any streamed body is forwarded.
  * Pass through TLS for hosts listed in tls-passthrough.txt (tls_clienthello →
    ignore_connection). Those tunnels keep end-to-end HTTP/2 keepalives so Agent idle
    waits are not cut by upstream edge timeouts under SSL inspection. ignore_hosts stays
    empty in CRITICAL_OPTIONS; only the explicit passthrough list may skip interception.
"""

import json
import os
import re
import sys
from urllib.parse import unquote_plus

READ_METHODS = frozenset({"GET", "HEAD"})
WRITE_METHODS = frozenset({"POST", "PUT", "OPTIONS", "PATCH", "DELETE"})

# Headers that can turn a read method into a write. On an origin that honors them a GET
# is effectively a write, so evaluate such requests against the write allowlist.
OVERRIDE_HEADERS = ("X-HTTP-Method-Override", "X-HTTP-Method", "X-Method-Override")

# Characters the origin may treat as delimiters. When the proxy and the origin disagree
# on how to interpret a path, the path pattern restriction can itself be bypassed
# (";" is a path parameter, "#" is a fragment).
UNSAFE_PATH_CHARS = ("#", ";", "\\")

SAFE_PORTS = frozenset({80, 443})

PROXY_PORT = 3128
PROXY_SELF_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

READ_ALLOWLIST_PATH = os.environ.get("READ_ALLOWLIST", "/etc/proxy/read-allowlist.txt")
WRITE_ALLOWLIST_PATH = os.environ.get("WRITE_ALLOWLIST", "/etc/proxy/write-allowlist.txt")
GRAPHQL_ENDPOINTS_PATH = os.environ.get("GRAPHQL_ENDPOINTS", "/etc/proxy/graphql-endpoints.txt")
TLS_PASSTHROUGH_PATH = os.environ.get("TLS_PASSTHROUGH", "/etc/proxy/tls-passthrough.txt")

# The "every domain" entry, usable only in read-allowlist.txt.
# Allowing it in write-allowlist would open up writes entirely, so it is rejected at load
# time. It is rejected in graphql-endpoints too: if every destination were treated as
# GraphQL, a POST to any domain would pass on the read allowlist alone simply by carrying
# a body shaped like a query.
WILDCARD = "*"

# The domain field of an allowlist entry. Typos such as "*.example.com" or
# "https://example.com" would merely fail to match and go unnoticed, so reject them during
# validation and abort startup.
DOMAIN_RE = re.compile(r"^\.?[a-z0-9_-]+(\.[a-z0-9_-]+)*$")

# Upper bound on a GraphQL body handed to the parser. Do not let oversized input reach it.
MAX_GRAPHQL_BODY = 256 * 1024

# Bound repeated query decoding to keep method-override detection linear in request size.
# Four layers cover the proxy/origin decoding chains this policy is designed to defend.
MAX_OVERRIDE_DECODE_PASSES = 4

# Critical options verified at startup. Some have permissive defaults, so do not start if
# they have changed unintentionally (never silently skip a check).
CRITICAL_OPTIONS = {
    # When true, non-HTTP traffic inside a CONNECT passes through as raw TCP without
    # reaching request(); this addon defines no TCP message hook
    "rawtcp": False,
    # When true, WebSocket frames after the 101 bypass request(). This addon defines no
    # WebSocket message hook; request() rejects the handshake, and this option blocks such
    # data at a second layer in case a check is missed
    "websocket": False,
    # When true, upstream certificate verification is skipped
    "ssl_insecure": False,
    # When nonempty, the listed hosts pass through without being intercepted. Keep empty
    # so passthrough is only possible via tls-passthrough.txt in this addon.
    "ignore_hosts": [],
    "allow_hosts": [],
    "mode": ["regular"],
}

# Each list is read once at startup (changes take effect on restart).
READ_ALLOWED = ()
WRITE_ALLOWED = ()
GRAPHQL_ENDPOINTS = ()
TLS_PASSTHROUGH = ()


# -----------------------------------------------------------------------------
# Loading and matching the allowlists
# -----------------------------------------------------------------------------
def parse_entry(line, allow_wildcard=False):
    """Convert "<domain> [path-pattern]" into (domain, path_pattern|None)."""
    fields = line.split()
    if len(fields) > 2:
        raise ValueError("too many fields (domain and path pattern only): " + line)

    domain = fields[0].lower().rstrip(".")
    path_pattern = fields[1] if len(fields) == 2 else None

    if domain == WILDCARD:
        if not allow_wildcard:
            raise ValueError('"*" is not allowed here (it would match every domain): ' + line)
    elif not DOMAIN_RE.match(domain):
        # This covers "*.example.com" (confused with the leading-dot notation),
        # "https://example.com" and "example.com/path" (the path is whitespace
        # separated). All of them would silently become entries that never match, so
        # fail at startup.
        raise ValueError("malformed domain: " + line)

    if path_pattern is not None:
        if path_pattern.count(WILDCARD) > 1:
            raise ValueError("a path pattern may contain at most one *: " + line)
        if WILDCARD in path_pattern and not (
            path_pattern.startswith(WILDCARD) or path_pattern.endswith(WILDCARD)
        ):
            raise ValueError("* in a path pattern is allowed only at the start or end: " + line)
        if not path_pattern.startswith(("/", WILDCARD)):
            raise ValueError("a path pattern must start with / or *: " + line)

    return domain, path_pattern


def load_allowlist(path, allow_wildcard=False):
    """Load an allowlist. One entry per line; everything after # is a comment."""
    entries = []
    with open(path) as handle:
        for number, raw in enumerate(handle, 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            try:
                entries.append(parse_entry(line, allow_wildcard))
            except ValueError as exc:
                raise ValueError(path + ":" + str(number) + ": " + str(exc)) from exc
    return tuple(entries)


def load_passthrough(path):
    """Load TLS passthrough domains. Domain-only lines; path patterns are rejected."""
    entries = []
    with open(path) as handle:
        for number, raw in enumerate(handle, 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            try:
                domain, path_pattern = parse_entry(line, allow_wildcard=False)
            except ValueError as exc:
                raise ValueError(path + ":" + str(number) + ": " + str(exc)) from exc
            if path_pattern is not None:
                raise ValueError(
                    path + ":" + str(number)
                    + ": path patterns are not allowed in tls-passthrough: " + line
                )
            entries.append((domain, None))
    return tuple(entries)


def match_domain(host, pattern):
    """A leading dot matches both subdomains and the apex; no dot means an exact match."""
    if pattern == WILDCARD:
        return True
    host = (host or "").lower().rstrip(".")
    if pattern.startswith("."):
        return host == pattern[1:] or host.endswith(pattern)
    return host == pattern


def match_passthrough_host(host, entries=None):
    """True if host matches a TLS passthrough domain entry."""
    if entries is None:
        entries = TLS_PASSTHROUGH
    host = (host or "").lower().rstrip(".")
    for domain, _path_pattern in entries or ():
        if match_domain(host, domain):
            return True
    return False


def _has_dot_segment(path):
    """True if splitting the path on / yields a "." or ".." segment."""
    return any(segment in (".", "..") for segment in path.split("/"))


def is_safe_raw_path(path):
    """True unless the path uses a representation the origin could reinterpret.

    The proxy forwards the raw path unchanged, so if the origin resolves dot segments or
    truncates at a delimiter, the string the proxy matched and the resource actually
    accessed diverge. For example, "github.com /login/*" would also prefix-match
    "/login/../o/r.git/git-receive-pack".

    Denying rather than normalizing here is deliberate: normalization would not remove the
    interpretation gap, because what reaches the origin is still the raw path. The policy
    is to refuse anything that is not in a form we can decide on.

    The query string is stripped before matching and does not affect path routing, so it
    is out of scope. %2f (an encoded slash) is used legitimately, for instance in gh
    branch names (feature%2Fbar). Representations that produce . or .. segments once %2f
    is decoded (/login/..%2fo/..., /login/foo%2f..%2f..) are traversals and are denied.
    """
    target = (path or "").split("?", 1)[0]
    if any(char in target for char in UNSAFE_PATH_CHARS):
        return False
    if any(char <= " " or char == "\x7f" for char in target):
        return False
    lowered = target.lower()
    # %2e = dot, %25 = percent (smuggling via double encoding),
    # %5c = backslash (some origins treat it as a delimiter)
    if "%2e" in lowered or "%25" in lowered or "%5c" in lowered:
        return False
    if _has_dot_segment(target):
        return False
    # Deny if dot segments appear once %2f is read as a slash. Values such as
    # feature%2Fbar, which add a separator without producing . or .., are allowed.
    if "%2f" in lowered:
        decoded = lowered.replace("%2f", "/")
        if _has_dot_segment(decoded):
            return False
    return True


def match_path(path, pattern):
    """Match against a path pattern. * is a glob allowed only at the start or end.

    Matching is case-sensitive and is done on the path with the query string removed.
    URL paths may be case-sensitive at the origin, so folding case here would create
    another interpretation gap.
    """
    if pattern is None or pattern == WILDCARD:
        return True
    path = (path or "").split("?", 1)[0]
    if pattern.startswith(WILDCARD):
        return path.endswith(pattern[1:])
    if pattern.endswith(WILDCARD):
        return path.startswith(pattern[:-1])
    return path == pattern


def match_any(host, path, entries):
    """True if any entry matches (both the domain and the path)."""
    for domain, path_pattern in entries or ():
        if match_domain(host, domain) and match_path(path, path_pattern):
            return True
    return False


def has_method_override(headers):
    """True if a header that overrides the method is present."""
    return any(headers.get(name) for name in OVERRIDE_HEADERS)


def has_query_method_override(path):
    """True if the query asks for a method override via _method= (Rails etc.).

    Query parsers may accept "&" or ";" separators and URL-decode at different layers.
    Inspect a fixed number of decoded forms of the complete query so encoded keys and
    separators cannot disguise an override, without attacker-controlled decode loops.
    """
    query = (path or "").split("?", 1)
    if len(query) < 2:
        return False
    candidate = query[1]
    for _ in range(MAX_OVERRIDE_DECODE_PASSES + 1):
        for part in re.split(r"[&;]", candidate):
            if part.split("=", 1)[0].lower() == "_method":
                return True
        decoded = unquote_plus(candidate)
        if decoded == candidate:
            break
        candidate = decoded
    return False


def is_websocket_upgrade(headers):
    """True if the request is an upgrade to WebSocket."""
    return "websocket" in (headers.get("Upgrade", "") or "").lower()


# -----------------------------------------------------------------------------
# GraphQL
# -----------------------------------------------------------------------------
def graphql_operation(body):
    """Parse a GraphQL body and return ("query"|"write"|None, reason).

    None means the body could not be classified, and the caller denies it (fail-closed).
    The body is interpreted with a GraphQL parser rather than by string matching, so a
    case where "mutation" merely appears inside a string literal in a query is not
    misclassified.
    """
    if not body:
        return None, "graphql_no_body"
    if len(body) > MAX_GRAPHQL_BODY:
        return None, "graphql_body_too_large"

    try:
        # Deferred import: the caller denies the request when classification is
        # impossible. Keep the module importable without graphql-core (for the unit
        # tests of path handling and the like).
        from graphql import OperationType, parse
    except Exception:  # noqa: BLE001 - without the parser we cannot classify
        return None, "graphql_parser_unavailable"

    try:
        payload = json.loads(body)
    except Exception:  # noqa: BLE001
        return None, "graphql_invalid_json"

    # Batch requests (arrays) are supported as well.
    items = payload if isinstance(payload, list) else [payload]
    if not items:
        return None, "graphql_empty_batch"

    kind = "query"
    reason = "graphql_query"
    for item in items:
        if not isinstance(item, dict):
            return None, "graphql_invalid_payload"
        query = item.get("query")
        if not isinstance(query, str):
            return None, "graphql_no_query"
        try:
            document = parse(query)
        except Exception:  # noqa: BLE001
            return None, "graphql_parse_error"

        operations = [
            d for d in document.definitions if getattr(d, "operation", None) is not None
        ]
        if not operations:
            return None, "graphql_no_operation"
        for operation in operations:
            # Regardless of which operation operationName selects, treat the document as
            # a write if it contains even one non-query operation (a decision that does
            # not depend on which one is chosen).
            if operation.operation is not OperationType.QUERY:
                kind = "write"
                reason = "graphql_" + operation.operation.value

    return kind, reason


# -----------------------------------------------------------------------------
# Decision logic
# -----------------------------------------------------------------------------
def decide(method, host, path, port, read_allowed=None, write_allowed=None,
           graphql_endpoints=None, body=b"", method_override=False):
    """Pure function returning (allowed, reason). All decisions are made here.

    body is used only for the GraphQL inspection; no other decision looks at it.
    method_override indicates whether a header overriding the method was present.
    The _method= query parameter is detected from path inside this function (callers do
    not need to pass it).
    """
    if read_allowed is None:
        read_allowed = READ_ALLOWED
    if write_allowed is None:
        write_allowed = WRITE_ALLOWED
    if graphql_endpoints is None:
        graphql_endpoints = GRAPHQL_ENDPOINTS
    method = (method or "").upper()

    # 1. Port
    if port not in SAFE_PORTS:
        return False, "unsafe_port"

    # 2. Deny path representations the origin could reinterpret; they would render the
    #    path pattern restrictions meaningless
    if not is_safe_raw_path(path):
        return False, "unsafe_path"

    # 3. A method override can disguise a write as a read, so decide immediately against
    #    the write allowlist. Do not let later GraphQL classification turn it back into a
    #    read based on its body.
    overridden = (
        method_override or has_query_method_override(path)
    ) and method in READ_METHODS
    if overridden:
        if match_any(host, path, write_allowed):
            return True, "write_allowed_domains"
        return False, "method_override_denied"

    # 4. Read methods
    if method in READ_METHODS:
        if match_any(host, path, read_allowed):
            return True, "read_allowed_domains"
        return False, "read_not_allowed"

    # 5. Neither read nor write (TRACE etc.)
    if method not in WRITE_METHODS:
        return False, "method_not_allowed"

    # 6. GraphQL uses the same endpoint for queries and non-query operations, so inspect
    #    the body to determine which allowlist applies.
    if method == "POST" and match_any(host, path, graphql_endpoints):
        kind, reason = graphql_operation(body)
        if kind is None:
            return False, reason
        if kind == "query":
            # If writes are allowed, queries are also allowed (write access implies read access)
            if match_any(host, path, read_allowed) or match_any(host, path, write_allowed):
                return True, reason
            return False, "read_not_allowed"
        if match_any(host, path, write_allowed):
            return True, reason
        # Reusing the ordinary default_deny would hide the fact that this was a non-query
        # operation and might prompt an operator to add the whole domain to write-allowlist,
        # so use a distinct reason.
        return False, reason + "_denied"

    # 7. Write methods
    if match_any(host, path, write_allowed):
        return True, "write_allowed_domains"

    # 8. Deny by default
    return False, "default_deny"


# -----------------------------------------------------------------------------
# Blocking Host spoofing
# -----------------------------------------------------------------------------
def strip_port(value):
    """Drop :port from a Host header.

    Handles both bracketed IPv6 ("[::1]:443") and bare IPv6 ("::1"). Naively splitting the
    latter on ":" yields an empty string and breaks the comparison.
    """
    value = (value or "").strip().lower().rstrip(".")
    if value.startswith("["):
        return value.split("]")[0].lstrip("[")
    if value.count(":") == 1:
        return value.split(":")[0]
    return value


def host_mismatch(conn_host, host_header):
    """True if the Host header disagrees with the actual connection target.

    Blocks attempts to connect to a disallowed destination while claiming an allowed
    domain in Host. A missing Host header does not count as a mismatch (the decision itself
    is made against the connection target, so this is safe).
    """
    if not host_header:
        return False
    return strip_port(host_header) != strip_port(conn_host)


def conn_host(flow):
    """The destination used for the decision: the connection target, not the Host header.

    For plaintext HTTP the request hook runs before the connection is established, so the
    address does not exist yet. In that case use request.host (the authority of the
    absolute URI). mitmproxy connects to that same value afterwards, so the destination
    used for the decision matches the one actually connected to. This is not the same as
    pretty_host, which prefers the Host header.

    If neither is available, return None and let the caller deny (never decide without
    knowing the destination).
    """
    address = getattr(getattr(flow, "server_conn", None), "address", None)
    if address:
        return address[0]
    return getattr(flow.request, "host", None)


# -----------------------------------------------------------------------------
# mitmproxy hooks
# -----------------------------------------------------------------------------
DENY_BODY = (
    "<html><body id=ERR_ACCESS_DENIED><h1>ERROR</h1>"
    "<p><b>Access Denied.</b></p>"
    "<p>Access control configuration prevents your request from being allowed "
    "at this time.</p>"
    "<p>Generated by proxy-devcontainer</p></body></html>"
)


def _log(verdict, flow, host, reason):
    """Audit log. Records the real destination used for the decision as well as the Host
    header, which can be spoofed."""
    request_obj = flow.request
    header = getattr(request_obj, "host_header", None) or request_obj.headers.get("Host", "")
    spoof = "" if strip_port(header) == strip_port(host) else " host_header=" + header
    print(
        verdict + " " + request_obj.method + " " + host + request_obj.path
        + " reason=" + reason + spoof,
        flush=True,
    )


def _deny(flow, host, reason):
    from mitmproxy import http

    _log("DENIED/403", flow, host, reason)
    flow.response = http.Response.make(
        403,
        DENY_BODY,
        {"Content-Type": "text/html; charset=utf-8", "X-Proxy-Deny-Reason": reason},
    )


def _describe(entries):
    parts = []
    for domain, path_pattern in entries:
        parts.append(domain if path_pattern is None else domain + " " + path_pattern)
    return ", ".join(parts) if parts else "(empty)"


def running():
    """Verify critical options and allowlists after startup; abort if anything is wrong."""
    global READ_ALLOWED, WRITE_ALLOWED, GRAPHQL_ENDPOINTS, TLS_PASSTHROUGH

    from mitmproxy import ctx

    problems = []
    for name, expected in CRITICAL_OPTIONS.items():
        actual = getattr(ctx.options, name, "(option does not exist)")
        if actual != expected:
            problems.append(name + ": expected=" + repr(expected) + " actual=" + repr(actual))

    try:
        READ_ALLOWED = load_allowlist(READ_ALLOWLIST_PATH, allow_wildcard=True)
    except Exception as exc:  # noqa: BLE001
        problems.append("failed to load read-allowlist: " + str(exc))

    try:
        WRITE_ALLOWED = load_allowlist(WRITE_ALLOWLIST_PATH)
    except Exception as exc:  # noqa: BLE001
        problems.append("failed to load write-allowlist: " + str(exc))

    try:
        GRAPHQL_ENDPOINTS = load_allowlist(GRAPHQL_ENDPOINTS_PATH)
    except Exception as exc:  # noqa: BLE001
        problems.append("failed to load graphql-endpoints: " + str(exc))

    try:
        TLS_PASSTHROUGH = load_passthrough(TLS_PASSTHROUGH_PATH)
    except Exception as exc:  # noqa: BLE001
        problems.append("failed to load tls-passthrough: " + str(exc))

    if problems:
        print("[policy] aborting startup: critical preconditions are not met:", flush=True)
        for problem in problems:
            print("[policy]   - " + problem, flush=True)
        sys.stdout.flush()
        # Exit immediately rather than listen in an unverified state
        os._exit(1)

    read_summary = _describe(READ_ALLOWED)
    if any(domain == WILDCARD and pattern is None for domain, pattern in READ_ALLOWED):
        read_summary += " (all domains allowed; edit read-allowlist.txt to narrow this)"
    print("[policy] ok: read=" + read_summary, flush=True)
    print("[policy] ok: write=" + _describe(WRITE_ALLOWED), flush=True)
    print("[policy] ok: graphql=" + _describe(GRAPHQL_ENDPOINTS), flush=True)
    print("[policy] ok: tls_passthrough=" + _describe(TLS_PASSTHROUGH), flush=True)


def tls_clienthello(data):
    """Skip SSL Bump for hosts listed in tls-passthrough.txt.

    Decision uses the CONNECT target only. If SNI disagrees with that target, keep
    intercepting (do not passthrough). If the target cannot be determined, keep
    intercepting so later hooks can still deny.
    """
    try:
        server = getattr(getattr(data, "context", None), "server", None)
        address = getattr(server, "address", None)
        if not address:
            return
        host = address[0]
        port = address[1] if len(address) > 1 else None
        sni = getattr(getattr(data, "client_hello", None), "sni", None)
        if sni and host_mismatch(host, sni):
            return
        if not match_passthrough_host(host):
            return
        data.ignore_connection = True
        where = str(host) + ((":" + str(port)) if port is not None else "")
        print("PASSTHROUGH " + where, flush=True)
    except Exception as exc:  # noqa: BLE001 - keep intercepting when undecidable
        print("tls_clienthello error (intercepting): " + repr(exc), flush=True)


def requestheaders(flow):
    """Decide from headers before any body is forwarded; stream only when allowed.

    mitmproxy forwards a streamed request body as it arrives, so enabling stream before a
    deny would let a rejected request reach the origin. GraphQL endpoints keep buffering so
    request() can inspect the body. Must run here rather than in request(): by then the
    body has already been buffered (or streamed).
    """
    try:
        request_obj = flow.request

        # Healthcheck is answered in request(); do not stream it.
        if request_obj.port == PROXY_PORT and strip_port(request_obj.host) in PROXY_SELF_HOSTS:
            return

        host = conn_host(flow)
        if not host:
            # Keep buffering; request() denies with missing_server_address.
            return

        header = getattr(request_obj, "host_header", None) or request_obj.headers.get("Host", "")
        if host_mismatch(host, header):
            _deny(flow, host, "host_mismatch")
            return

        if is_websocket_upgrade(request_obj.headers):
            _deny(flow, host, "websocket_not_allowed")
            return

        path = request_obj.path or ""
        # Body inspection requires buffering; request() will decide.
        if match_any(host, path, GRAPHQL_ENDPOINTS):
            return

        allowed, reason = decide(
            request_obj.method, host, path, request_obj.port, body=b"",
            method_override=has_method_override(request_obj.headers),
        )
        if not allowed:
            _deny(flow, host, reason)
            return

        flow.request.stream = True
    except Exception as exc:  # noqa: BLE001 - fail closed before any body is forwarded
        print("DENIED/403 requestheaders policy error: " + repr(exc), flush=True)
        try:
            _deny(flow, "-", "policy_error")
        except Exception:  # noqa: BLE001
            flow.kill()


def responseheaders(flow):
    """Stream every response body on bumped connections. The policy never inspects them.

    Without this, long-lived streams on SSL-bumped hosts arrive only after the upstream
    finishes. Cursor Agent hosts listed in tls-passthrough.txt skip interception and do
    not use this path.
    """
    try:
        flow.response.stream = True
    except Exception:  # noqa: BLE001
        pass


def http_connect(flow):
    """Check the port at CONNECT time (the equivalent of squid's Safe_ports /
    CONNECT !SSL_ports).

    The destination domain is not examined here. CONNECT is allowed to every domain and
    the decision is made after decryption.
    """
    try:
        from mitmproxy import http

        port = flow.request.port
        if port not in SAFE_PORTS:
            print(
                "DENIED/403 CONNECT " + flow.request.host + ":" + str(port) + " reason=unsafe_port",
                flush=True,
            )
            flow.response = http.Response.make(
                403,
                DENY_BODY,
                {
                    "Content-Type": "text/html; charset=utf-8",
                    "X-Proxy-Deny-Reason": "unsafe_port",
                },
            )
    except Exception as exc:  # noqa: BLE001 - fall back to denying when undecidable
        print("DENIED/403 connect policy error: " + repr(exc), flush=True)
        flow.kill()


def request(flow):
    # Wrap everything so that an exception cannot let a request through (fail-closed).
    try:
        from mitmproxy import http

        # requestheaders may already have denied (or the connect hook). Do not reopen.
        if flow.response is not None:
            return

        request_obj = flow.request

        # Requests to the proxy itself (healthcheck) are outside the policy scope. This is
        # equivalent to squid's manager ACL.
        if request_obj.port == PROXY_PORT and strip_port(request_obj.host) in PROXY_SELF_HOSTS:
            flow.response = http.Response.make(200, b"ok\n", {"Content-Type": "text/plain"})
            return

        # Always decide against the destination actually being connected to. Deny the
        # request if that destination cannot be determined.
        host = conn_host(flow)
        if not host:
            _deny(flow, "-", "missing_server_address")
            return

        # Block Host spoofing
        header = getattr(request_obj, "host_header", None) or request_obj.headers.get("Host", "")
        if host_mismatch(host, header):
            _deny(flow, host, "host_mismatch")
            return

        # Everything after the 101 bypasses request(), and this addon has no WebSocket
        # message hook, so stop WebSocket at the handshake.
        if is_websocket_upgrade(request_obj.headers):
            _deny(flow, host, "websocket_not_allowed")
            return

        # The body is used only for the GraphQL inspection. If it cannot be read, leave it
        # empty and fall back to denying.
        try:
            body = request_obj.get_content(strict=False) or b""
        except Exception:  # noqa: BLE001
            body = b""

        allowed, reason = decide(
            request_obj.method, host, request_obj.path, request_obj.port, body=body,
            # decide() detects the _method= query parameter from the path
            method_override=has_method_override(request_obj.headers),
        )
        if allowed:
            _log("ALLOW", flow, host, reason)
            return
        _deny(flow, host, reason)
    except Exception as exc:  # noqa: BLE001 - fall back to denying when undecidable
        print("DENIED/403 policy error: " + repr(exc), flush=True)
        try:
            _deny(flow, "-", "policy_error")
        except Exception:  # noqa: BLE001
            flow.kill()
