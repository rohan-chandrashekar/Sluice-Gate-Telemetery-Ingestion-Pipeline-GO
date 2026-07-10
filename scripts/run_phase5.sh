#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/sdk/go/bin:$HOME/go/bin:$HOME/.local/bin:$PATH"

mkdir -p bin docs/charts

go build -o bin/report ./cmd/report

echo "=== phase5: regenerating README results table + charts ==="
./bin/report

echo "phase5 complete"
