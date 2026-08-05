#!/usr/bin/env bash
# Build the stripped static agent binaries.
#
# Usage: ./build.sh [version]
# The version defaults to the git description, then to "dev".
set -euo pipefail

cd "$(dirname "$0")"

VERSION="${1:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
OUT="dist"
PKG="./cmd/observant-agent"
LDFLAGS="-s -w -X main.version=${VERSION}"

TARGETS=(
  "linux/amd64"
  "linux/arm64"
  "darwin/arm64"
)

rm -rf "${OUT}"
mkdir -p "${OUT}"

for target in "${TARGETS[@]}"; do
  GOOS="${target%%/*}"
  GOARCH="${target##*/}"
  BIN="${OUT}/observant-agent-${GOOS}-${GOARCH}"
  echo "building ${BIN}"
  CGO_ENABLED=0 GOOS="${GOOS}" GOARCH="${GOARCH}" \
    go build -trimpath -ldflags "${LDFLAGS}" -o "${BIN}" "${PKG}"
done

echo
echo "version ${VERSION}"
echo
printf '%-40s %12s %10s\n' "BINARY" "BYTES" "SIZE"
for f in "${OUT}"/*; do
  size=$(wc -c < "${f}" | tr -d ' ')
  human=$(awk -v b="${size}" 'BEGIN { printf "%.2f MiB", b / 1048576 }')
  printf '%-40s %12s %10s\n' "$(basename "${f}")" "${size}" "${human}"
done
