#!/usr/bin/env bash
# Generate a self-signed root CA (idempotent).
# If both the private PEM and public certificate already exist, they are not regenerated,
# so the CA stays consistent with the trust store on the cursor side. If either artifact is
# missing, the pair is regenerated.
#
# The private key and the public certificate are kept in separate places:
#   - private key (/private/ca)         : a mitm-only volume, never mounted into cursor.
#   - public certificate (/shared/ca)   : shared with cursor and mounted read-only there;
#                                         used to populate the trust store.
# Keeping the key on the shared volume and protecting it with chmod 600 works under Docker,
# but not under rootless Podman with userns keep-id, where mitm's root maps to cursor's
# uid 1000 and the key becomes readable. Separating the volumes puts it out of cursor's
# reach in either environment.
#
# mitmproxy uses mitmproxy-ca.pem (the concatenated private key and certificate) in its
# configuration directory as the CA.
set -euo pipefail

CA_DIR=/shared/ca
KEY_DIR=/private/ca
CRT="$CA_DIR/proxy-ca.crt"
CA_PEM="$KEY_DIR/mitmproxy-ca.pem"

mkdir -p "$CA_DIR" "$KEY_DIR"

# Migrating from the squid setup: an old CA left on the shared volume is no longer needed,
# so remove it.
if [[ -f "$CA_DIR/squid-ca.crt" ]]; then
  echo "[gen-ca] removing legacy squid CA from the shared volume."
  rm -f "$CA_DIR/squid-ca.crt"
fi
if [[ -f "$CA_DIR/squid-ca.key" ]]; then
  echo "[gen-ca] removing legacy squid key from the shared volume."
  rm -f "$CA_DIR/squid-ca.key"
fi

if [[ -f "$CA_PEM" && -f "$CRT" ]]; then
  echo "[gen-ca] existing CA found; skipping generation."
  exit 0
fi

echo "[gen-ca] generating new root CA..."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
  -keyout "$tmp/key.pem" \
  -out    "$tmp/cert.pem" \
  -subj "/CN=DevContainer Proxy Root CA/O=local-dev"

cat "$tmp/key.pem" "$tmp/cert.pem" > "$CA_PEM"
cp "$tmp/cert.pem" "$CRT"

# The private key is root-only (600). On top of that, this volume is not mounted into cursor.
chmod 600 "$CA_PEM"
# The public certificate is 644 so cursor can import it into the trust store.
chmod 644 "$CRT"
echo "[gen-ca] done."
