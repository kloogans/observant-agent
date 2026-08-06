#!/usr/bin/env sh
# check.sh — the gates for the collector, run where you are sitting.
#
# GitHub Actions is not in this project's path. A self-hosted runner did not
# help during the 2026-08-06 outage, because GitHub still hands out the job and
# marks it finished; an idle machine you own cannot start work on its own.
set -eu
cd "$(dirname "$0")"
echo "== vet"
go vet ./...
echo "== test"
go test -count=1 ./...
echo "== build"
CGO_ENABLED=0 go build ./...
echo "== every gate passed"
