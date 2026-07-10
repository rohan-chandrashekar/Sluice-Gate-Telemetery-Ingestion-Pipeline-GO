# Demo script (45-60s screen recording)

A short screen-recording script for demonstrating the two correctness properties that matter most
in this project: zero event loss across a broker restart, and exactly-once persistence via dedup.
Both are real, scripted tests (`scripts/run_phase1.sh`, `scripts/run_phase2.sh`) — this is a
narration guide for recording them, not a separate demo path.

## Setup (not recorded, or trimmed from the final cut)

```sh
cd sluice
make build
(cd deploy && docker compose --profile phase2 up -d)
# wait for kafka/timescaledb/redis healthy - see scripts/run_phase2.sh's wait_* functions
./bin/topicinit -brokers localhost:9092 -topics telemetry.losstest,telemetry.dedup -partitions 12
```

## Segment 1 — zero loss across a broker restart (~25s)

Narration: "This pipeline never silently drops an event, even if the Kafka broker restarts
mid-produce. Watch the count."

```sh
# terminal 1: start a consumer counting into a plain in-memory sink
./bin/consumer -brokers localhost:9092 -topic telemetry.losstest -group demo-losstest -sink memory

# terminal 2: produce exactly 200,000 events with deterministic idempotency keys
./bin/losstest -brokers localhost:9092 -topic telemetry.losstest -count 200000 -concurrency 16 &

# terminal 3, a couple seconds in: restart the broker mid-produce
(cd deploy && docker compose restart kafka)
```

Cut to: terminal 1's log line once it settles — `final memory sink accepted=200000 consumed=200000`.
Narration: "200,000 in, 200,000 out, across a live broker restart."

## Segment 2 — exactly-once at the sink via dedup (~25s)

Narration: "If the same batch gets replayed — a common failure mode after a consumer crash — the
dedup layer makes sure it only lands once."

```sh
# terminal 1: consumer with dedup + timescale sink
./bin/consumer -brokers localhost:9092 -topic telemetry.dedup -group demo-dedup -sink timescale \
  -timescale-dsn postgres://sluice:sluice@localhost:5432/sluice -dedup -redis-addr localhost:6379

# terminal 2: produce a batch, then produce the SAME batch again
./bin/losstest -brokers localhost:9092 -topic telemetry.dedup -count 20000 -concurrency 8
./bin/losstest -brokers localhost:9092 -topic telemetry.dedup -count 20000 -concurrency 8
```

Cut to: a `psql` count query showing exactly 20,000 rows despite 40,000 events produced —

```sh
docker compose exec timescaledb psql -U sluice -d sluice -c \
  "SELECT count(*) FROM telemetry_events WHERE idempotency_key LIKE 'loss-%';"
```

Narration: "40,000 events produced, 20,000 distinct keys, 20,000 rows."

## Teardown (not recorded)

```sh
(cd deploy && docker compose --profile phase2 down)
docker volume rm deploy_kafka-data deploy_timescale-data
```

## terminal-cast reference commands

For an asciinema/terminalizer recording instead of a screen capture, these are the exact commands
in order, with no narration interleaved:

```sh
./bin/consumer -brokers localhost:9092 -topic telemetry.losstest -group demo-losstest -sink memory
./bin/losstest -brokers localhost:9092 -topic telemetry.losstest -count 200000 -concurrency 16
(cd deploy && docker compose restart kafka)
./bin/consumer -brokers localhost:9092 -topic telemetry.dedup -group demo-dedup -sink timescale -timescale-dsn postgres://sluice:sluice@localhost:5432/sluice -dedup -redis-addr localhost:6379
./bin/losstest -brokers localhost:9092 -topic telemetry.dedup -count 20000 -concurrency 8
./bin/losstest -brokers localhost:9092 -topic telemetry.dedup -count 20000 -concurrency 8
docker compose exec timescaledb psql -U sluice -d sluice -c "SELECT count(*) FROM telemetry_events WHERE idempotency_key LIKE 'loss-%';"
```
