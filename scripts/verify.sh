#!/usr/bin/env bash
# Verification gate: shell lint + Go fmt/vet/build/test.
set -euo pipefail
cd "$(dirname "$0")/.."
mapfile -t scripts < <(find . -name '*.sh' -not -path './.git/*')
shellcheck "${scripts[@]}"
if [ -f go.mod ]; then
  unformatted=$(gofmt -l . || true)
  if [ -n "$unformatted" ]; then
    echo "gofmt needed on:" >&2
    echo "$unformatted" >&2
    exit 1
  fi
  go vet ./...
  go build ./...
  # The race detector needs cgo (a C compiler). Use it wherever one is present
  # — notably CI, which is where concurrent-access regressions are cheapest to
  # catch — and fall back to a plain run on hosts without a compiler.
  cc=$(go env CC)
  if command -v "$cc" >/dev/null 2>&1 || command -v gcc >/dev/null 2>&1 || command -v clang >/dev/null 2>&1; then
    CGO_ENABLED=1 go test -race ./...
  else
    echo "note: no C compiler found; running tests without -race" >&2
    go test ./...
  fi
fi

echo "llm-gateway verify OK"
