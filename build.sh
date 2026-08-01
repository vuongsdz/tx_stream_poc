#!/usr/bin/env bash
# Build all binaries used by ecosystem.config.js.
#
#   ./build.sh
#   pm2 restart ecosystem.config.js   # or: pm2 start ecosystem.config.js
set -euo pipefail

cd "$(dirname "$0")"

echo "building tx_stream_clock_poc…"
go build -o tx_stream_clock_poc .

echo "building blockcompare…"
go build -o blockcompare ./cmd/blockcompare

echo "building blockobserver…"
go build -o blockobserver ./cmd/blockobserver

echo "building txvsblock…"
go build -o txvsblock ./cmd/txvsblock

echo "building txvsblockobserver…"
go build -o txvsblockobserver ./cmd/txvsblockobserver

echo "building txcompare…"
go build -o txcompare ./cmd/txcompare

echo "building txcompareobserver…"
go build -o txcompareobserver ./cmd/txcompareobserver

echo "done."
