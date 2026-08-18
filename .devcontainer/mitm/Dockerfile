# An SSL-Bump-capable forward proxy (mitmproxy).
# The decision logic lives in policy.py (an addon).
#
# mitmproxy is installed with uv, as the project officially recommends.
# The version is pinned by uv.lock; at build time the lock is exported to a requirements
# file and passed as a constraint to uv tool install, so only the locked versions are
# actually installed.
#
# See the comments in pyproject.toml for the update procedure. After updating, always run
# .devcontainer/tests/regression-suite.sh
# (some mitmproxy options have permissive defaults that can change between versions).
FROM debian:trixie-slim

# Python comes from uv as well (the distribution's python3 is not installed).
# Pinning the interpreter improves dependency reproducibility and insulates it from
# distribution Python updates; it does not make the entire image build reproducible.
ARG PYTHON_VERSION=3.13.14

# openssl is used to generate the CA; curl is used for the healthcheck and to fetch uv.
RUN apt-get update && apt-get install -y --no-install-recommends \
      openssl \
      ca-certificates \
      curl \
 && rm -rf /var/lib/apt/lists/*

# uv - following the official standalone installer (always the latest version)
# https://docs.astral.sh/uv/getting-started/installation/
#   UV_INSTALL_DIR    : set a fixed installation directory (the default is under $HOME)
#   UV_NO_MODIFY_PATH : do not let it rewrite the shell profile
# uv itself is a build-time tool and plays no part in runtime policy decisions. uv.lock and
# PYTHON_VERSION pin the Python dependency graph and interpreter, but uv, the Debian base
# image, and apt packages remain unpinned, so this is not a fully reproducible image build.
# With `curl | sh`, sh would swallow a curl failure and the build would still succeed, so
# the download and the execution are chained with `&&` and finally verified with
# `uv --version`.
RUN curl -fsSL https://astral.sh/uv/install.sh -o /tmp/uv-install.sh \
 && env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh /tmp/uv-install.sh \
 && rm -f /tmp/uv-install.sh \
 && uv --version

# Set fixed locations for uv (the defaults vary with the user's home directory)
ENV UV_TOOL_DIR=/opt/uv/tools \
    UV_TOOL_BIN_DIR=/usr/local/bin \
    UV_PYTHON_INSTALL_DIR=/opt/uv/python

COPY pyproject.toml uv.lock /opt/proxy-lock/

# Install the uv-managed Python, then export uv.lock to a requirements file (with hashes)
# and run uv tool install with it as a constraint.
# --frozen keeps uv.lock from being re-resolved.
# --with graphql-core: policy.py uses it to inspect GraphQL bodies, so it has to live in
#                      the same tool environment as mitmproxy.
RUN uv python install "${PYTHON_VERSION}" \
 && cd /opt/proxy-lock \
 && uv export --frozen --no-dev --no-emit-project -o /tmp/constraints.txt \
 && uv tool install --python "${PYTHON_VERSION}" --constraints /tmp/constraints.txt \
      --with graphql-core mitmproxy \
 && rm -f /tmp/constraints.txt \
 && mitmdump --version \
 && /opt/uv/tools/mitmproxy/bin/python -c "import graphql; print('graphql-core', graphql.version)"

# An entry point for invoking the Python interpreter from the mitmproxy environment,
# for example from tests.
# A symlink breaks the venv's prefix resolution, so use a wrapper script.
RUN printf '#!/bin/sh\nexec /opt/uv/tools/mitmproxy/bin/python "$@"\n' > /usr/local/bin/proxy-python \
 && chmod 0755 /usr/local/bin/proxy-python

COPY --chmod=0755 gen-ca.sh     /usr/local/bin/gen-ca.sh
COPY --chmod=0755 entrypoint.sh /usr/local/bin/entrypoint.sh
COPY policy.py      /opt/proxy/policy.py
COPY test_policy.py /opt/proxy/test_policy.py

# Mount points for the shared CA (public certificate), the CA private key (mitm only) and
# the allowlists.
RUN mkdir -p /shared/ca /private/ca /etc/proxy

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
