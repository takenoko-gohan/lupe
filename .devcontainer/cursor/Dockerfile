# The Cursor Agent CLI execution environment. Cursor Agent is distributed as a
# self-contained package, so no Node.js runtime is needed. Plain Debian is used as a
# minimal base.
FROM debian:trixie-slim

ARG USERNAME=cursor

ENV DEBIAN_FRONTEND=noninteractive
# Put the natively installed agent (~/.local/bin) and the mise shims on PATH in every
# context. Having the shims in ENV makes mise-managed tools visible to non-interactive
# shells and to processes started by Cursor Agent (activating them in the shell rc alone
# would only cover interactive shells).
ENV PATH=/home/${USERNAME}/.local/share/mise/shims:/home/${USERNAME}/.local/bin:${PATH}

# Basic tools plus zsh. ca-certificates provides update-ca-certificates and the
# /usr/local/share/ca-certificates directory.
# wget is used in the official GitHub CLI instructions for Debian; curl is used by the
# Cursor Agent installer.
# python3 is installed to give the agent an environment for running scripts.
# trixie is an externally managed environment (PEP 668), so python3-venv is also installed
# to allow virtual environments instead of installing packages into the system with pip
# (python3-venv provides the ensurepip support; python3-pip also provides system pip).
# build-essential provides core C/C++ build tools such as gcc, g++, make, and libc6-dev.
# cmake, pkg-config, and gdb are commonly used for build configuration, dependency
# resolution, and debugging.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      wget \
      git \
      sudo \
      zsh \
      fzf \
      less \
      jq \
      procps \
      python3 \
      python3-pip \
      python3-venv \
      build-essential \
      cmake \
      pkg-config \
      gdb \
 && rm -rf /var/lib/apt/lists/*

# A non-root working user (debian-slim has none, so create it).
RUN groupadd --gid 1000 ${USERNAME} \
 && useradd  --uid 1000 --gid 1000 --create-home --shell /usr/bin/zsh ${USERNAME}

# GitHub CLI (gh) - following the official Debian instructions
# https://github.com/cli/cli/blob/trunk/docs/install_linux.md#debian-ubuntu-linux-raspberry-pi-os-apt
RUN mkdir -p -m 755 /etc/apt/keyrings \
 && out=$(mktemp) && wget -nv -O"$out" https://cli.github.com/packages/githubcli-archive-keyring.gpg \
 && cat "$out" | tee /etc/apt/keyrings/githubcli-archive-keyring.gpg > /dev/null \
 && chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
 && mkdir -p -m 755 /etc/apt/sources.list.d \
 && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
      | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
 && apt-get update && apt-get install -y --no-install-recommends gh \
 && rm -rf /var/lib/apt/lists/*

# The CA import script, plus NOPASSWD sudo limited to the cursor user
COPY --chmod=0755 trust-ca.sh   /usr/local/bin/trust-ca.sh
COPY --chmod=0755 entrypoint.sh /usr/local/bin/entrypoint.sh
RUN echo "${USERNAME} ALL=(root) NOPASSWD: /usr/local/bin/trust-ca.sh" \
      > /etc/sudoers.d/trust-ca && chmod 0440 /etc/sudoers.d/trust-ca

# Mount points for the persistent volumes. Making cursor the owner preserves that ownership
# when a named volume is initialized.
RUN mkdir -p /commandhistory /home/${USERNAME}/.cursor \
      /home/${USERNAME}/.config/cursor /home/${USERNAME}/.config/gh /shared/ca \
 && chown -R ${USERNAME}:${USERNAME} /commandhistory /home/${USERNAME}/.cursor \
      /home/${USERNAME}/.config

# Persist the shell history in /commandhistory (for both bash and zsh).
# For fully delegated operation, run `agent --force` (or `--yolo`) explicitly each time
# (no alias adds either mode automatically).
RUN printf '%s\n' \
      "export HISTFILE=/commandhistory/.shell_history" \
      >> /home/${USERNAME}/.bashrc \
 && printf '%s\n' \
      "export HISTFILE=/commandhistory/.shell_history" \
      >> /home/${USERNAME}/.zshrc \
 && chown ${USERNAME}:${USERNAME} /home/${USERNAME}/.bashrc /home/${USERNAME}/.zshrc

USER ${USERNAME}

# mise - a userspace language runtime manager (Rust / Go / Node / Java / Kotlin / Gradle, etc.).
# It requires neither sudo nor apt. The runtimes themselves are installed later with
# `mise use` (see the README).
# https://mise.jdx.dev/
# With `curl | sh`, sh could mask a curl failure, so download and execution are chained
# with `&&`, and the installation is verified with `mise --version`.
RUN curl -fsSL https://mise.run -o /tmp/mise-install.sh \
 && sh /tmp/mise-install.sh \
 && rm -f /tmp/mise-install.sh \
 && mise --version \
 && printf '%s\n' 'eval "$(mise activate bash --shims)"' >> /home/${USERNAME}/.bashrc \
 && printf '%s\n' 'eval "$(mise activate zsh --shims)"' >> /home/${USERNAME}/.zshrc

# Cursor Agent CLI - the official installer (run as the cursor user).
# https://cursor.com/docs/cli/installation
# It is installed at ~/.local/bin/agent (and ~/.local/bin/cursor-agent) with the package
# under ~/.local/share/cursor-agent/versions/<ver>. Both locations are outside the
# cursor-settings / cursor-auth volumes, so they do not conflict with them.
# Image-build downloads are outside the runtime proxy policy. Automatic background updates
# remain enabled; their GET requests to downloads.cursor.com use read-allowlist.txt.
# With `curl | bash`, bash would swallow a curl failure and the build would still succeed,
# so the download and execution are chained with `&&`; `agent --version` then verifies
# that the installation succeeded.
RUN curl -fsSL https://cursor.com/install -o /tmp/install.sh \
 && bash /tmp/install.sh \
 && rm -f /tmp/install.sh \
 && agent --version

WORKDIR /workspace

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["sleep", "infinity"]
