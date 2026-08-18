#!/usr/bin/env bash
# Entry point for the cursor container (runs as the cursor user).
#   1. Import the proxy CA into the system trust store (curl / gh / git / Node --use-system-ca)
#   2. Set the CA for git explicitly as an additional safeguard
#   3. If Java was installed with mise, import the proxy CA into its cacerts too
#      (Java/Gradle do not look at the OS CA store)
#   4. Run CMD (sleep infinity) to keep the container running
set -euo pipefail

CA=/shared/ca/proxy-ca.crt

# Wait for mitm to generate the CA (with depends_on: service_healthy, it normally exists already)
for i in $(seq 1 30); do
  [[ -f "$CA" ]] && break
  echo "[entrypoint] waiting for CA at $CA ... ($i)"
  sleep 1
done

# Import it into the system trust store (sudo only for the part that needs root)
sudo /usr/local/bin/trust-ca.sh

# Set the CA for git explicitly (curl, gh, git, and Cursor Agent itself trust the proxy CA
# through the system trust store populated by update-ca-certificates)
git config --global http.sslCAInfo "$CA" || true

# Disable hooks for Git commands run as cursor inside the container. This setting does not
# affect Git commands run by the host user. .git itself stays writable, so this does not
# prevent object tampering (it is accident prevention).
git config --global core.hooksPath /dev/null || true

# Java (mise) does not use the OS CA, so add the proxy CA to its cacerts when it is present.
# A missing Java installation, an already imported certificate, or a different Java version
# must not prevent startup.
if [[ -x "${HOME}/.local/bin/mise" ]]; then
  if java_home=$("${HOME}/.local/bin/mise" where java 2>/dev/null); then
    keytool_bin="${java_home}/bin/keytool"
    cacerts="${java_home}/lib/security/cacerts"
    if [[ -x "$keytool_bin" && -f "$cacerts" ]]; then
      "$keytool_bin" -importcert -noprompt -alias proxy-devcontainer-ca \
        -file "$CA" -keystore "$cacerts" -storepass changeit \
        >/dev/null 2>&1 || true
    fi
  fi
fi

exec "$@"
