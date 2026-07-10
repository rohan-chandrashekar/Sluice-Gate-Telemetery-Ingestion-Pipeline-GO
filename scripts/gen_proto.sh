#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export PATH="$HOME/sdk/go/bin:$HOME/go/bin:$HOME/.local/bin:$PATH"

protoc \
  --go_out=. --go_opt=module=github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO \
  --go-grpc_out=. --go-grpc_opt=module=github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO \
  proto/sluice/v1/telemetry.proto

echo "generated gen/sluice/v1"
