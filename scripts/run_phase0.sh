#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/sdk/go/bin:$HOME/go/bin:$HOME/.local/bin:$PATH"

mkdir -p bin bench/results
go build -o bin/gateway ./cmd/gateway
go build -o bin/loadgen ./cmd/loadgen

WORKERS=64
QUEUE=20000
LISTEN=":50051"

for CONCURRENCY in 8 32 128; do
  echo "=== phase0 sweep: concurrency=${CONCURRENCY} ==="

  ./bin/gateway -listen "${LISTEN}" -workers "${WORKERS}" -queue "${QUEUE}" > "/tmp/sluice_gateway_phase0_${CONCURRENCY}.log" 2>&1 &
  GW_PID=$!

  for i in $(seq 1 20); do
    if (exec 3<>/dev/tcp/localhost/50051) 2>/dev/null; then
      exec 3<&- 3>&-
      break
    fi
    sleep 0.25
  done

  ./bin/loadgen \
    -target localhost:50051 \
    -duration 30s \
    -warmup 5s \
    -concurrency "${CONCURRENCY}" \
    -batch-size 100 \
    -devices 10000 \
    -sink-label memory \
    -gateway-workers "${WORKERS}" \
    -gateway-queue "${QUEUE}" \
    -results-prefix "phase0_c${CONCURRENCY}"

  kill -INT "${GW_PID}"
  wait "${GW_PID}" 2>/dev/null || true
  tail -5 "/tmp/sluice_gateway_phase0_${CONCURRENCY}.log"
  echo
done

echo "phase0 sweep complete, results in bench/results/phase0_*.json"
