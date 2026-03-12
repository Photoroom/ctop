#!/usr/bin/env bash
set -euo pipefail

REPO="${CTOP_INSTALL_REPO:-Photoroom/ctop}"
VERSION="${CTOP_VERSION:-latest}"
INSTALL_DIR="${CTOP_INSTALL_DIR:-$HOME/.local/bin}"

os="$(uname -s)"
arch="$(uname -m)"

case "${os}/${arch}" in
  Linux/x86_64)
    target="x86_64-unknown-linux-musl"
    ;;
  *)
    echo "ctop installer currently supports Linux x86_64 only." >&2
    echo "Detected: ${os}/${arch}" >&2
    exit 1
    ;;
esac

asset="ctop-${target}.tar.gz"
if [[ "${VERSION}" == "latest" ]]; then
  url="https://github.com/${REPO}/releases/latest/download/${asset}"
else
  url="https://github.com/${REPO}/releases/download/${VERSION}/${asset}"
fi

fetch() {
  local src="$1"
  local dst="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$src" -o "$dst"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$dst" "$src"
  else
    echo "Need curl or wget to install ctop." >&2
    exit 1
  fi
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

archive="${tmpdir}/${asset}"
fetch "$url" "$archive"
tar -xzf "$archive" -C "$tmpdir"

install -d "$INSTALL_DIR"
install -m 0755 "${tmpdir}/ctop" "${INSTALL_DIR}/ctop"

echo "Installed ctop to ${INSTALL_DIR}/ctop"
if [[ ":$PATH:" != *":${INSTALL_DIR}:"* ]]; then
  echo "Add ${INSTALL_DIR} to PATH if needed."
fi
