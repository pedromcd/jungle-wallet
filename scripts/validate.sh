#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."

printf '\n[1/7] Toolchain and dependencies\n'
go version
if ! go mod tidy; then
  echo 'BLOCKED: dependency resolution failed; check network/DNS access to GOPROXY.' >&2
  exit 2
fi
if [[ ! -s go.sum ]]; then echo 'FAIL: go.sum missing' >&2; exit 1; fi

printf '\n[2/7] Formatting\n'
mapfile -d '' gofiles < <(find . -type f -name '*.go' -print0)
if ((${#gofiles[@]} == 0)); then echo 'FAIL: no Go files' >&2; exit 1; fi
unformatted="$(gofmt -l "${gofiles[@]}")"
if [[ -n "$unformatted" ]]; then printf 'FAIL: unformatted files:\n%s\n' "$unformatted" >&2; exit 1; fi

printf '\n[3/7] Unit tests\n'
go test -count=1 ./...
printf '\n[4/7] Race detector\n'
go test -race -count=1 ./...
printf '\n[5/7] Static analysis\n'
go vet ./...
go vet -tags 'integration e2e' ./...
printf '\n[6/7] Integration (PostgreSQL)\n'
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  make test-integration
else
  echo 'NOT RUN: Docker daemon unavailable; integration tests are mandatory for final sign-off.'
fi
printf '\n[7/7] End-to-end (opt-in)\n'
if [[ "${RUN_E2E:-0}" == 1 ]]; then
  if ! command -v docker >/dev/null 2>&1; then echo 'FAIL: RUN_E2E=1 but Docker is missing' >&2; exit 1; fi
  make test-e2e
else
  echo 'NOT RUN: set RUN_E2E=1 to execute full-stack E2E tests.'
fi
printf '\nLocal checks completed. Full acceptance requires PostgreSQL integration and E2E PASS.\n'
