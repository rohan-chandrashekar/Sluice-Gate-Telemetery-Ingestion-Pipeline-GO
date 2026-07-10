#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/sdk/go/bin:$HOME/go/bin:$HOME/.local/bin:$PATH"

mkdir -p bin bench/results

DSN="postgres://sluice:sluice@localhost:5432/sluice"

wait_kafka_healthy() {
  for i in $(seq 1 40); do
    status=$(docker inspect --format='{{.State.Health.Status}}' sluice-kafka 2>/dev/null || echo "missing")
    if [ "$status" = "healthy" ]; then return 0; fi
    sleep 3
  done
  echo "kafka never became healthy" >&2
  return 1
}

wait_timescale_ready() {
  for i in $(seq 1 40); do
    if (cd deploy && docker compose exec -T timescaledb pg_isready -U sluice) >/dev/null 2>&1; then
      return 0
    fi
    sleep 3
  done
  echo "timescale never became ready" >&2
  return 1
}

wait_redis_ready() {
  for i in $(seq 1 40); do
    if (cd deploy && docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG); then
      return 0
    fi
    sleep 2
  done
  echo "redis never became ready" >&2
  return 1
}

kill_and_wait() {
  local pid="$1"
  kill -INT "${pid}" 2>/dev/null || return 0
  for i in $(seq 1 20); do
    if ! kill -0 "${pid}" 2>/dev/null; then return 0; fi
    sleep 1
  done
  echo "pid ${pid} did not exit after SIGINT within 20s, sending SIGKILL" >&2
  kill -9 "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
}

(cd deploy && docker compose --profile phase2 up -d)
echo "waiting for infra (kafka, timescaledb, redis)..."
wait_kafka_healthy
wait_timescale_ready
wait_redis_ready

go build -o bin/gateway ./cmd/gateway
go build -o bin/consumer ./cmd/consumer
go build -o bin/loadgen ./cmd/loadgen
go build -o bin/topicinit ./cmd/topicinit
go build -o bin/losstest ./cmd/losstest
go build -o bin/injectmalformed ./cmd/injectmalformed
go build -o bin/topiccount ./cmd/topiccount

./bin/topicinit -brokers localhost:9092 \
  -topics telemetry.events,telemetry.events.c8,telemetry.events.c32,telemetry.events.c128,telemetry.events.dlq,telemetry.dedup \
  -partitions 12 -replication 1

WORKERS=32
QUEUE=5000

echo "=== phase2 sweep (gateway -> kafka -> consumer[dedup+dlq] -> timescale) ==="
echo "each concurrency round uses its own topic (telemetry.events.cN) so a fresh consumer"
echo "group never has to replay a previous round's backlog (see DECISIONS.md)."
for CONCURRENCY in 8 32 128; do
  echo "--- concurrency=${CONCURRENCY} ---"
  ROUND_TOPIC="telemetry.events.c${CONCURRENCY}"

  ./bin/gateway -sink kafka -brokers localhost:9092 -topic "${ROUND_TOPIC}" \
    -listen :50051 -workers "${WORKERS}" -queue "${QUEUE}" \
    > "/tmp/sluice_gw_phase2_${CONCURRENCY}.log" 2>&1 &
  GW_PID=$!

  ./bin/consumer -brokers localhost:9092 -topic "${ROUND_TOPIC}" \
    -group "sluice-sink-p2-c${CONCURRENCY}" -sink timescale \
    -timescale-dsn "${DSN}" -batch 500 -flush-interval 2s -max-retries 3 \
    -dedup -redis-addr localhost:6379 -dedup-ttl 10m \
    -dlq -dlq-topic telemetry.events.dlq \
    > "/tmp/sluice_cons_phase2_${CONCURRENCY}.log" 2>&1 &
  CONS_PID=$!

  sleep 3

  ./bin/loadgen \
    -target localhost:50051 \
    -duration 30s -warmup 5s \
    -concurrency "${CONCURRENCY}" -batch-size 100 -devices 10000 \
    -sink-label timescale -real-infra=true \
    -gateway-workers "${WORKERS}" -gateway-queue "${QUEUE}" \
    -results-prefix "phase2_c${CONCURRENCY}"

  echo "draining..."
  sleep 10

  kill_and_wait "${GW_PID}"
  kill_and_wait "${CONS_PID}"
  tail -3 "/tmp/sluice_gw_phase2_${CONCURRENCY}.log"
  tail -3 "/tmp/sluice_cons_phase2_${CONCURRENCY}.log"
  echo
done

echo "=== phase2 DEDUP TEST: replay a batch twice, assert row count = distinct key count ==="
DEDUP_TOPIC=telemetry.dedup
DEDUP_GROUP=sluice-dedup-test
DEDUP_COUNT=20000

./bin/consumer -brokers localhost:9092 -topic "${DEDUP_TOPIC}" -group "${DEDUP_GROUP}" -sink timescale \
  -timescale-dsn "${DSN}" -batch 500 -flush-interval 2s -max-retries 3 \
  -dedup -redis-addr localhost:6379 -dedup-ttl 10m \
  -dlq -dlq-topic telemetry.events.dlq \
  > /tmp/sluice_cons_deduptest.log 2>&1 &
CONS_PID=$!
sleep 2

echo "producing batch (first pass, ${DEDUP_COUNT} distinct keys)..."
./bin/losstest -brokers localhost:9092 -topic "${DEDUP_TOPIC}" -count "${DEDUP_COUNT}" -concurrency 8
echo "producing the SAME batch again (replay, same deterministic keys)..."
./bin/losstest -brokers localhost:9092 -topic "${DEDUP_TOPIC}" -count "${DEDUP_COUNT}" -concurrency 8

echo "waiting for consumer to drain and flush..."
sleep 15
kill_and_wait "${CONS_PID}"
tail -8 /tmp/sluice_cons_deduptest.log

ROW_COUNT_RAW=$(cd deploy && docker compose exec -T timescaledb \
  psql -U sluice -d sluice -t -A -c "SELECT count(*) FROM telemetry_events WHERE idempotency_key LIKE 'loss-%';")
ROW_COUNT=$(echo "${ROW_COUNT_RAW}" | tr -d '[:space:]')

echo "distinct keys expected=${DEDUP_COUNT} actual row count=${ROW_COUNT}"
DEDUP_PASS=false
if [ "${ROW_COUNT}" = "${DEDUP_COUNT}" ]; then
  echo "DEDUP TEST PASS: row count matches distinct key count"
  DEDUP_PASS=true
else
  echo "DEDUP TEST FAIL: row count ${ROW_COUNT} != distinct keys ${DEDUP_COUNT}"
fi

echo "=== phase2 DLQ TEST: inject malformed records, assert DLQ + pipeline keeps flowing ==="
DLQ_TEST_TOPIC=telemetry.events
DLQ_GROUP=sluice-dlqtest
MALFORMED_COUNT=25

./bin/consumer -brokers localhost:9092 -topic "${DLQ_TEST_TOPIC}" -group "${DLQ_GROUP}" -sink timescale \
  -timescale-dsn "${DSN}" -batch 500 -flush-interval 2s -max-retries 3 \
  -dlq -dlq-topic telemetry.events.dlq \
  > /tmp/sluice_cons_dlqtest.log 2>&1 &
CONS_PID=$!
sleep 2

./bin/gateway -sink kafka -brokers localhost:9092 -topic "${DLQ_TEST_TOPIC}" \
  -listen :50051 -workers 8 -queue 1000 \
  > /tmp/sluice_gw_dlqtest.log 2>&1 &
GW_PID=$!
sleep 2

echo "sending valid events before injection..."
./bin/loadgen -target localhost:50051 -duration 5s -warmup 1s -concurrency 4 -batch-size 20 -devices 100 \
  -sink-label timescale -real-infra=true -results-prefix phase2_dlqtest_before

echo "injecting ${MALFORMED_COUNT} malformed records directly into ${DLQ_TEST_TOPIC}..."
./bin/injectmalformed -brokers localhost:9092 -topic "${DLQ_TEST_TOPIC}" -count "${MALFORMED_COUNT}"

echo "sending valid events after injection (pipeline should keep flowing)..."
./bin/loadgen -target localhost:50051 -duration 5s -warmup 1s -concurrency 4 -batch-size 20 -devices 100 \
  -sink-label timescale -real-infra=true -results-prefix phase2_dlqtest_after

sleep 8
kill_and_wait "${GW_PID}"
kill_and_wait "${CONS_PID}"
tail -15 /tmp/sluice_cons_dlqtest.log

DLQ_COUNT=$(./bin/topiccount -brokers localhost:9092 -topic telemetry.events.dlq \
  -group "dlqcount-$(date +%s)" -idle-timeout 5s 2>&1 | grep -oP 'count=\K[0-9]+' || echo 0)
AFTER_CONSUMED=$(grep -oP 'final consumed=\K[0-9]+' /tmp/sluice_cons_dlqtest.log || echo 0)

echo "dlq message count observed=${DLQ_COUNT} (expected >= ${MALFORMED_COUNT}), valid events consumed=${AFTER_CONSUMED}"

DLQ_PASS=false
if [ "${DLQ_COUNT}" -ge "${MALFORMED_COUNT}" ] && [ "${AFTER_CONSUMED}" -gt 0 ]; then
  echo "DLQ TEST PASS: malformed records reached the DLQ and the pipeline kept processing valid events"
  DLQ_PASS=true
else
  echo "DLQ TEST FAIL"
fi

RESULT_FILE="bench/results/phase2_correctness_$(date -u +%Y%m%dT%H%M%SZ).json"
cat > "${RESULT_FILE}" <<EOF
{
  "phase": "phase2_correctness",
  "dedup_test": {
    "produced_count_x2": $((DEDUP_COUNT * 2)),
    "distinct_keys": ${DEDUP_COUNT},
    "row_count": ${ROW_COUNT},
    "pass": ${DEDUP_PASS}
  },
  "dlq_test": {
    "malformed_injected": ${MALFORMED_COUNT},
    "dlq_count_observed": ${DLQ_COUNT},
    "valid_events_consumed_after_injection": ${AFTER_CONSUMED},
    "pass": ${DLQ_PASS}
  },
  "machine_tag": "Intel MacBook Pro, quad-core i5-1038NG7 (8 threads), 16GB RAM, Ubuntu 20.04, native Docker"
}
EOF
echo "wrote ${RESULT_FILE}"

(cd deploy && docker compose --profile phase2 down)
docker volume rm deploy_kafka-data deploy_timescale-data 2>/dev/null || true

if [ "${DEDUP_PASS}" != "true" ] || [ "${DLQ_PASS}" != "true" ]; then
  exit 1
fi

echo "phase2 complete"
