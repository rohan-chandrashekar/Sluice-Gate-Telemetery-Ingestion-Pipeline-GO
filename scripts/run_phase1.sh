#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/sdk/go/bin:$HOME/go/bin:$HOME/.local/bin:$PATH"

mkdir -p bin bench/results

wait_kafka_healthy() {
  for i in $(seq 1 40); do
    status=$(docker inspect --format='{{.State.Health.Status}}' sluice-kafka 2>/dev/null || echo "missing")
    if [ "$status" = "healthy" ]; then
      return 0
    fi
    sleep 3
  done
  echo "kafka never became healthy" >&2
  return 1
}

(cd deploy && docker compose up -d kafka)
echo "waiting for kafka..."
wait_kafka_healthy

go build -o bin/gateway ./cmd/gateway
go build -o bin/consumer ./cmd/consumer
go build -o bin/loadgen ./cmd/loadgen
go build -o bin/topicinit ./cmd/topicinit
go build -o bin/losstest ./cmd/losstest

./bin/topicinit -brokers localhost:9092 \
  -topics telemetry.events,telemetry.events.dlq,telemetry.losstest \
  -partitions 12 -replication 1

WORKERS=32
QUEUE=5000

echo "=== phase1 sweep (gateway -> kafka -> consumer -> memory sink) ==="
for CONCURRENCY in 8 32 128; do
  echo "--- concurrency=${CONCURRENCY} ---"

  ./bin/gateway -sink kafka -brokers localhost:9092 -topic telemetry.events \
    -listen :50051 -workers "${WORKERS}" -queue "${QUEUE}" \
    > "/tmp/sluice_gw_phase1_${CONCURRENCY}.log" 2>&1 &
  GW_PID=$!

  ./bin/consumer -brokers localhost:9092 -topic telemetry.events \
    -group "sluice-sink-c${CONCURRENCY}" -sink memory \
    > "/tmp/sluice_cons_phase1_${CONCURRENCY}.log" 2>&1 &
  CONS_PID=$!

  sleep 3

  ./bin/loadgen \
    -target localhost:50051 \
    -duration 30s -warmup 5s \
    -concurrency "${CONCURRENCY}" -batch-size 100 -devices 10000 \
    -sink-label kafka -real-infra=true \
    -gateway-workers "${WORKERS}" -gateway-queue "${QUEUE}" \
    -results-prefix "phase1_c${CONCURRENCY}"

  echo "draining consumer lag..."
  sleep 10

  kill -INT "${GW_PID}" "${CONS_PID}" 2>/dev/null || true
  wait "${GW_PID}" 2>/dev/null || true
  wait "${CONS_PID}" 2>/dev/null || true
  tail -3 "/tmp/sluice_gw_phase1_${CONCURRENCY}.log"
  tail -3 "/tmp/sluice_cons_phase1_${CONCURRENCY}.log"
  echo
done

echo "=== phase1 LOSS TEST: produce known count, restart broker mid-run, assert zero loss ==="
LOSS_TOPIC=telemetry.losstest
LOSS_GROUP=sluice-losstest
LOSS_COUNT=200000

./bin/consumer -brokers localhost:9092 -topic "${LOSS_TOPIC}" -group "${LOSS_GROUP}" -sink memory \
  > /tmp/sluice_cons_losstest.log 2>&1 &
CONS_PID=$!
sleep 2

./bin/losstest -brokers localhost:9092 -topic "${LOSS_TOPIC}" -count "${LOSS_COUNT}" -concurrency 16 \
  > /tmp/sluice_losstest_producer.log 2>&1 &
PROD_PID=$!

sleep 3
echo "restarting kafka broker mid-run..."
(cd deploy && docker compose restart kafka)

echo "waiting for kafka to come back healthy..."
wait_kafka_healthy

echo "waiting for producer to finish (retries through the restart)..."
wait "${PROD_PID}"
tail -3 /tmp/sluice_losstest_producer.log

echo "waiting for consumer to drain lag to zero..."
sleep 20

kill -INT "${CONS_PID}" 2>/dev/null || true
wait "${CONS_PID}" 2>/dev/null || true

FINAL_LINE=$(grep "final memory sink accepted" /tmp/sluice_cons_losstest.log | tail -1 || true)
echo "${FINAL_LINE}"

CONSUMED=$(echo "${FINAL_LINE}" | grep -oP 'consumed=\K[0-9]+' || echo "-1")

RESULT_FILE="bench/results/phase1_losstest_$(date -u +%Y%m%dT%H%M%SZ).json"
if [ "${CONSUMED}" = "${LOSS_COUNT}" ]; then
  echo "LOSS TEST PASS: consumed=${CONSUMED} == produced=${LOSS_COUNT}"
  PASS=true
else
  echo "LOSS TEST FAIL: consumed=${CONSUMED} != produced=${LOSS_COUNT}"
  PASS=false
fi

cat > "${RESULT_FILE}" <<EOF
{
  "phase": "phase1_losstest",
  "produced": ${LOSS_COUNT},
  "consumed": ${CONSUMED},
  "pass": ${PASS},
  "machine_tag": "Intel MacBook Pro, quad-core i5-1038NG7 (8 threads), 16GB RAM, Ubuntu 20.04, native Docker"
}
EOF
echo "wrote ${RESULT_FILE}"

(cd deploy && docker compose down)

if [ "${PASS}" != "true" ]; then
  exit 1
fi

echo "phase1 complete"
