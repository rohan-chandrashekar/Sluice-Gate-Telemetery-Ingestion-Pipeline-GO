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

wait_http_ok() {
  local url="$1"
  for i in $(seq 1 40); do
    if curl -sf -o /dev/null "${url}"; then return 0; fi
    sleep 2
  done
  echo "http endpoint ${url} never became ready" >&2
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

free_disk_mb() {
  df -Pm / | awk 'NR==2{print $4}'
}

(cd deploy && docker compose --profile phase3 up -d)
echo "waiting for infra (kafka, timescaledb, redis, prometheus, grafana)..."
wait_kafka_healthy
wait_timescale_ready
wait_redis_ready
wait_http_ok "http://localhost:9090/-/ready"
wait_http_ok "http://localhost:3000/api/health"

FREE_MB=$(free_disk_mb)
echo "free disk after infra is up: ${FREE_MB}MB"
if [ "${FREE_MB}" -lt 1000 ]; then
  echo "BLOCKED: only ${FREE_MB}MB free before the sweep even starts, below the 1000MB safety margin." >&2
  echo "Command to unblock: free space manually (docker system prune -af --volumes, remove old kernels)" >&2
  echo "then rerun: ./scripts/run_phase3.sh" >&2
  (cd deploy && docker compose --profile phase3 down)
  exit 1
fi

go build -o bin/gateway ./cmd/gateway
go build -o bin/consumer ./cmd/consumer
go build -o bin/loadgen ./cmd/loadgen
go build -o bin/topicinit ./cmd/topicinit

./bin/topicinit -brokers localhost:9092 \
  -topics telemetry.events.c8,telemetry.events.c32,telemetry.events.c128,telemetry.events.backpressure,telemetry.events.dlq \
  -partitions 12 -replication 1

WORKERS=32
QUEUE=5000

echo "=== phase3 sweep (gateway+consumer with /metrics, Prometheus scraping every 5s) ==="
echo "NOTE: this sweep runs a shorter window (10s vs the 30s used in Phase 1/2) and deletes each"
echo "round's topic + truncates the Timescale table immediately after that round, purely to fit"
echo "this disk-constrained machine (see RUN_STATUS.md for the incident that made this necessary)."
echo "Phase 1/2's RESULTS.md already has the authoritative 30s throughput numbers for this same"
echo "pipeline; this sweep exists to prove the /metrics + Prometheus wiring works under load, not"
echo "to re-derive those numbers."
for CONCURRENCY in 8 32 128; do
  echo "--- concurrency=${CONCURRENCY} ---"
  ROUND_TOPIC="telemetry.events.c${CONCURRENCY}"

  FREE_MB=$(free_disk_mb)
  echo "free disk before this round: ${FREE_MB}MB"
  if [ "${FREE_MB}" -lt 700 ]; then
    echo "BLOCKED: only ${FREE_MB}MB free, below the 700MB safety margin. Stopping the sweep here." >&2
    echo "Command to unblock: free space manually (docker system prune -af --volumes) then rerun:" >&2
    echo "  ./scripts/run_phase3.sh" >&2
    (cd deploy && docker compose --profile phase3 down)
    docker volume rm deploy_kafka-data deploy_timescale-data 2>/dev/null || true
    exit 1
  fi

  ./bin/gateway -sink kafka -brokers localhost:9092 -topic "${ROUND_TOPIC}" \
    -listen :50051 -workers "${WORKERS}" -queue "${QUEUE}" -metrics-addr :9100 \
    > "/tmp/sluice_gw_phase3_${CONCURRENCY}.log" 2>&1 &
  GW_PID=$!

  ./bin/consumer -brokers localhost:9092 -topic "${ROUND_TOPIC}" \
    -group "sluice-sink-p3-c${CONCURRENCY}" -sink timescale \
    -timescale-dsn "${DSN}" -batch 500 -flush-interval 2s -max-retries 3 \
    -dedup -redis-addr localhost:6379 -dedup-ttl 10m \
    -dlq -dlq-topic telemetry.events.dlq -metrics-addr :9101 \
    > "/tmp/sluice_cons_phase3_${CONCURRENCY}.log" 2>&1 &
  CONS_PID=$!

  sleep 3

  ./bin/loadgen \
    -target localhost:50051 \
    -duration 10s -warmup 3s \
    -concurrency "${CONCURRENCY}" -batch-size 100 -devices 10000 \
    -sink-label timescale -real-infra=true \
    -gateway-workers "${WORKERS}" -gateway-queue "${QUEUE}" \
    -results-prefix "phase3_c${CONCURRENCY}"

  echo "draining..."
  sleep 5

  kill_and_wait "${GW_PID}"
  kill_and_wait "${CONS_PID}"
  tail -3 "/tmp/sluice_gw_phase3_${CONCURRENCY}.log"
  tail -3 "/tmp/sluice_cons_phase3_${CONCURRENCY}.log"

  echo "cleaning up this round's data (topic delete + table truncate) to keep disk flat for the next round..."
  ./bin/topicinit -brokers localhost:9092 -topics "${ROUND_TOPIC}" -delete
  (cd deploy && docker compose exec -T timescaledb psql -U sluice -d sluice -c "TRUNCATE telemetry_events;") >/dev/null 2>&1 || true
  sleep 2
  echo
done

echo "recycling Kafka/Timescale data before the backpressure test (disk is tight on this machine;"
echo "the sweep's data has no further use and would otherwise accumulate under the heavier load below)..."
(cd deploy && docker compose stop kafka timescaledb)
(cd deploy && docker compose rm -f kafka timescaledb)
docker volume rm deploy_kafka-data deploy_timescale-data 2>/dev/null || true
(cd deploy && docker compose --profile phase3 up -d kafka timescaledb)
wait_kafka_healthy
wait_timescale_ready
./bin/topicinit -brokers localhost:9092 -topics telemetry.events.backpressure,telemetry.events.dlq -partitions 12 -replication 1

FREE_MB=$(free_disk_mb)
echo "free disk before backpressure test: ${FREE_MB}MB"
if [ "${FREE_MB}" -lt 800 ]; then
  echo "BLOCKED: only ${FREE_MB}MB free, below the 800MB safety margin for the backpressure test." >&2
  echo "Command to unblock: free space manually (e.g. docker system prune, remove old kernels/snaps)" >&2
  echo "then rerun: ./scripts/run_phase3.sh" >&2
  (cd deploy && docker compose --profile phase3 down)
  docker volume rm deploy_kafka-data deploy_timescale-data 2>/dev/null || true
  exit 1
fi

echo "=== phase3 BACKPRESSURE PROOF: drive load well above the sink ceiling, watch RSS/heap stay flat ==="
BP_TOPIC=telemetry.events.backpressure
BP_WORKERS=16
BP_QUEUE=2000

./bin/gateway -sink kafka -brokers localhost:9092 -topic "${BP_TOPIC}" \
  -listen :50051 -workers "${BP_WORKERS}" -queue "${BP_QUEUE}" -metrics-addr :9100 \
  > /tmp/sluice_gw_backpressure.log 2>&1 &
GW_PID=$!

./bin/consumer -brokers localhost:9092 -topic "${BP_TOPIC}" \
  -group sluice-sink-backpressure -sink timescale \
  -timescale-dsn "${DSN}" -batch 500 -flush-interval 2s -max-retries 3 \
  -dedup -redis-addr localhost:6379 -dedup-ttl 10m \
  -dlq -dlq-topic telemetry.events.dlq -metrics-addr :9101 \
  > /tmp/sluice_cons_backpressure.log 2>&1 &
CONS_PID=$!

sleep 3

SAMPLES_FILE="/tmp/sluice_backpressure_samples.csv"
echo "timestamp,rss_bytes,heap_alloc_bytes,queue_depth" > "${SAMPLES_FILE}"

sample_metrics() {
  while true; do
    METRICS=$(curl -s http://localhost:9100/metrics)
    RSS=$(echo "${METRICS}" | awk '/^process_resident_memory_bytes /{print $2}')
    HEAP=$(echo "${METRICS}" | awk '/^go_memstats_heap_alloc_bytes /{print $2}')
    QDEPTH=$(echo "${METRICS}" | awk '/^sluice_gateway_queue_depth /{print $2}')
    echo "$(date -u +%s),${RSS:-0},${HEAP:-0},${QDEPTH:-0}" >> "${SAMPLES_FILE}"
    sleep 2
  done
}
sample_metrics &
SAMPLER_PID=$!

echo "driving load at concurrency 128 (well above the observed real-infra ceiling of ~60k events/sec)..."
./bin/loadgen \
  -target localhost:50051 \
  -duration 20s -warmup 5s \
  -concurrency 128 -batch-size 100 -devices 10000 \
  -sink-label timescale -real-infra=true \
  -gateway-workers "${BP_WORKERS}" -gateway-queue "${BP_QUEUE}" \
  -results-prefix phase3_backpressure_load

kill -9 "${SAMPLER_PID}" 2>/dev/null || true
wait "${SAMPLER_PID}" 2>/dev/null || true

kill_and_wait "${GW_PID}"
kill_and_wait "${CONS_PID}"
tail -5 /tmp/sluice_gw_backpressure.log
tail -5 /tmp/sluice_cons_backpressure.log

echo "RSS/heap samples over the run:"
cat "${SAMPLES_FILE}"

BASELINE_RSS=$(awk -F, 'NR==2{print $2}' "${SAMPLES_FILE}")
PEAK_RSS=$(awk -F, 'NR>1{print $2}' "${SAMPLES_FILE}" | sort -n | tail -1)
BASELINE_HEAP=$(awk -F, 'NR==2{print $3}' "${SAMPLES_FILE}")
PEAK_HEAP=$(awk -F, 'NR>1{print $3}' "${SAMPLES_FILE}" | sort -n | tail -1)
PEAK_QDEPTH=$(awk -F, 'NR>1{print $4}' "${SAMPLES_FILE}" | sort -n | tail -1)

GROWTH_PASS=false
if [ -n "${BASELINE_RSS}" ] && [ -n "${PEAK_RSS}" ] && [ "${BASELINE_RSS}" != "0" ]; then
  GROWTH_RATIO=$(awk -v b="${BASELINE_RSS}" -v p="${PEAK_RSS}" 'BEGIN{printf "%.3f", p/b}')
  echo "RSS baseline=${BASELINE_RSS} peak=${PEAK_RSS} ratio=${GROWTH_RATIO}"
  if awk -v r="${GROWTH_RATIO}" 'BEGIN{exit !(r < 3.0)}'; then
    GROWTH_PASS=true
  fi
else
  GROWTH_RATIO="unknown"
fi

if [ "${GROWTH_PASS}" = "true" ]; then
  echo "BACKPRESSURE PROOF PASS: RSS stayed bounded (ratio ${GROWTH_RATIO} < 3.0x) despite queue depth peaking at ${PEAK_QDEPTH} (cap ${BP_QUEUE}) under sustained overload"
else
  echo "BACKPRESSURE PROOF FAIL or INCONCLUSIVE: ratio=${GROWTH_RATIO}"
fi

RESULT_FILE="bench/results/phase3_backpressure_$(date -u +%Y%m%dT%H%M%SZ).json"
SAMPLES_JSON=$(awk -F, 'NR>1{printf "%s{\"ts\":%s,\"rss_bytes\":%s,\"heap_alloc_bytes\":%s,\"queue_depth\":%s}", (NR>2?",":""), $1, $2, $3, $4}' "${SAMPLES_FILE}")

cat > "${RESULT_FILE}" <<EOF
{
  "phase": "phase3_backpressure",
  "config": {
    "gateway_workers": ${BP_WORKERS},
    "gateway_queue": ${BP_QUEUE},
    "loadgen_concurrency": 128,
    "loadgen_batch_size": 100
  },
  "baseline_rss_bytes": ${BASELINE_RSS:-0},
  "peak_rss_bytes": ${PEAK_RSS:-0},
  "rss_growth_ratio": "${GROWTH_RATIO}",
  "baseline_heap_alloc_bytes": ${BASELINE_HEAP:-0},
  "peak_heap_alloc_bytes": ${PEAK_HEAP:-0},
  "peak_queue_depth": ${PEAK_QDEPTH:-0},
  "pass": ${GROWTH_PASS},
  "samples": [${SAMPLES_JSON}],
  "machine_tag": "Intel MacBook Pro, quad-core i5-1038NG7 (8 threads), 16GB RAM, Ubuntu 20.04, native Docker"
}
EOF
echo "wrote ${RESULT_FILE}"

(cd deploy && docker compose --profile phase3 down)
docker volume rm deploy_kafka-data deploy_timescale-data 2>/dev/null || true
docker volume prune -f 2>/dev/null || true

if [ "${GROWTH_PASS}" != "true" ]; then
  exit 1
fi

echo "phase3 complete"
