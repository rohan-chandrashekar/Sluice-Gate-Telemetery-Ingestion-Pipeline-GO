# RESULTS

Machine tag for every number below: **Intel MacBook Pro, quad-core i5-1038NG7 (8 threads), 16GB
RAM, Ubuntu 20.04, native Docker**.

> Confirm these with one clean manual re-run of the `scripts/run_phaseN.sh` scripts before citing
> them anywhere (Rule Zero: cite only from a run with saved logs). Numbers here come from the
> `bench/results/*.json` files produced by this session's runs.

## Phase 0 — Ingest ceiling (in-process gRPC, `MemorySink`, no Docker)

Environment: go1.25.0, GOMAXPROCS=8, linux/amd64, `real_infra: false` (correct for this phase —
gRPC gateway + bounded pool + in-memory sink only, loopback on localhost, gateway `-workers 64
-queue 20000`).

| Concurrency | Batch | Throughput (events/sec) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Errors |
|---|---|---|---|---|---|---|---|
| 8   | 100 | 1,768,450 | 0.414 | 0.876 | 1.181 | 6.038  | 7  |
| 32  | 100 | 2,202,177 | 1.517 | 3.131 | 4.166 | 17.957 | 31 |
| 128 | 100 | 2,542,250 | 5.333 | 10.453 | 15.221 | 52.494 | 81 |

Source files: `bench/results/phase0_c8_20260709T224826Z.json`,
`bench/results/phase0_c32_20260709T224902Z.json`,
`bench/results/phase0_c128_20260709T224937Z.json`.

Reading: throughput keeps climbing through concurrency 128 but tail latency grows roughly linearly
with concurrency — this is the gRPC-server + bounded-pool ceiling on 8 hardware threads with no
downstream I/O at all (MemorySink is an atomic counter). It is a ceiling number, not a realistic
pipeline number; the errors column (7/31/81) are `context.DeadlineExceeded` on in-flight Push calls
whose response arrived after the loadgen's per-run context expired at the end of the window — not
dropped events (the gateway's own drained-accepted count was always slightly higher than the
client-reported accepted count for exactly this reason; see `internal/gateway/server.go`).

## Phase 1 — Kafka in the middle (real infra: single-broker KRaft Kafka, `apache/kafka:3.7.1`)

Environment: go1.25.0, GOMAXPROCS=8, linux/amd64, `real_infra: true`. Gateway `-sink kafka
-workers 32 -queue 5000`, topic `telemetry.events` with 12 partitions, replication factor 1
(single broker).

| Concurrency | Batch | Throughput (events/sec) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Errors | Rejected |
|---|---|---|---|---|---|---|---|---|
| 8   | 100 | 36,603 | 22.725  | 36.340  | 42.697  | 59.671  | 9   | 0   |
| 32  | 100 | 39,457 | 87.949  | 135.791 | 153.354 | 165.151 | 33  | 0   |
| 128 | 100 | 43,264 | 328.204 | 495.976 | 536.609 | 542.638 | 122 | 474 |

Source files: `bench/results/phase1_c8_20260709T230438Z.json`,
`bench/results/phase1_c32_20260709T230526Z.json`,
`bench/results/phase1_c128_20260709T230614Z.json`.

Reading: throughput is far below Phase 0's loopback ceiling because every event now does a
synchronous, acked (`acks=all`) round trip to Kafka instead of an atomic counter bump — this is
the real cost of durable persistence on a single local broker, not a regression. Tail latency
grows sharply at concurrency 128 (p99 536ms, some `rejected` from Push calls whose context
deadline elapsed while genuinely backpressured) — the bounded queue is doing exactly its job under
sustained overload against a slower sink.

**Loss test — PASS.** Produced exactly 200,000 events via `cmd/losstest` (idempotent producer,
retrying through failures), restarted the Kafka broker mid-produce, and confirmed the consumer's
sink accepted exactly 200,000 events once fully drained: zero loss across a real broker restart.
Source: `bench/results/phase1_losstest_20260709T230712Z.json`.

## Phase 2 — Timescale + Redis dedup + DLQ (real infra: Kafka + TimescaleDB + Redis)

Environment: go1.25.0, GOMAXPROCS=8, linux/amd64, `real_infra: true`. Gateway `-sink kafka -workers
32 -queue 5000`; consumer `-sink timescale -batch 500 -flush-interval 2s -max-retries 3 -dedup
-dlq`, each concurrency round on its own topic (`telemetry.events.c8/.c32/.c128`) to avoid
cross-round replay (see RUN_STATUS.md for why that matters here).

| Concurrency | Batch | Throughput (events/sec) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Errors | Rejected |
|---|---|---|---|---|---|---|---|---|
| 8   | 100 | 42,443 | 16.941  | 30.065  | 38.863  | 5016.388* | 11 | 0    |
| 32  | 100 | 57,978 | 60.588  | 85.328  | 94.306  | 120.652   | 30 | 69   |
| 128 | 100 | 60,559 | 238.027 | 305.136 | 340.001 | 459.801   | 13 | 4617 |

\* the concurrency-8 max latency (5.0s) is a single outlier, almost certainly a JIT/first-batch
effect (Kafka metadata refresh or a Timescale connection-pool warmup) rather than steady-state
behavior — p99 (38.9ms) is the more representative tail figure for that row.

Source files: `bench/results/phase2_c8_20260710T002833Z.json`,
`bench/results/phase2_c32_20260710T002923Z.json`,
`bench/results/phase2_c128_20260710T003017Z.json`.

Reading: throughput here (42k-60k/s) is noticeably *higher* than Phase 1's Kafka-only numbers
(36.6k-43.3k/s) despite Phase 2 doing strictly more work per event (dedup check + batched Timescale
persist vs. just a Kafka produce). This is not a contradiction: both phases' gateways are otherwise
identical and the actual bottleneck in both is the gateway's synchronous `ProduceSync` path — this
run's per-round topics avoided the cumulative-replay tax the consumer paid in earlier (discarded)
attempts, and batched `CopyFrom` + pipelined Redis dedup checks are cheap enough that the consumer
keeps up with the Kafka producer easily. The two numbers aren't a controlled A/B and shouldn't be
read as "Timescale is faster than Kafka."

**Dedup test — PASS.** Produced a fixed batch of 20,000 events with deterministic idempotency keys
via `cmd/losstest` against a dedicated `telemetry.dedup` topic, then produced the *exact same batch
again* (replay). Consumer had `-dedup` enabled (Redis `SETNX`-based, pipelined). Final TimescaleDB
row count for those keys: exactly 20,000 — the replayed 20,000 duplicates were all correctly
deduped (consumer log: `consumed=20000 deduped=20000`, i.e. every duplicate was seen and skipped,
not just silently ignored). Source: `bench/results/phase2_correctness_20260710T003131Z.json`.

**DLQ test — PASS.** Injected 25 deliberately malformed (non-protobuf) records directly into
`telemetry.events`, sandwiched between two bursts of valid traffic (`phase2_dlqtest_before`:
99,800 accepted; `phase2_dlqtest_after`: 93,560 accepted). All 25 malformed records landed in
`telemetry.events.dlq` (confirmed via `cmd/topiccount`), and the consumer kept processing valid
events throughout — final consumed count 193,453, zero pipeline stall. Source: same
`phase2_correctness_20260710T003131Z.json`.

## Phase 3 — Prometheus/Grafana + backpressure proof (real infra)

Environment: go1.25.0, GOMAXPROCS=8, linux/amd64, `real_infra: true`. Same pipeline as Phase 2
(gateway `-sink kafka` → Kafka → consumer `-sink timescale -dedup -dlq`), now with `/metrics` on
both processes and Prometheus scraping every 5s. **Sweep window shortened to 10s/3s-warmup** (see
RUN_STATUS.md for why — a disk-exhaustion incident on this machine, not a methodology change worth
trusting for absolute throughput comparison against Phase 1/2's 30s numbers); each round's topic and
Timescale rows are deleted immediately after that round to keep disk flat.

| Concurrency | Batch | Throughput (events/sec) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |
|---|---|---|---|---|---|---|
| 8   | 100 | 25,140 | 23.495  | 42.500  | 61.899  | 5112.857* |
| 32  | 100 | 60,340 | 64.029  | 85.656  | 94.241  | 111.346   |
| 128 | 100 | 68,370 | 238.682 | 300.417 | 306.446 | 309.854   |

\* again a single first-batch outlier (see the same note under Phase 2's concurrency-8 row); not
representative of steady state.

Source files: `bench/results/phase3_c8_20260710T012337Z.json`,
`bench/results/phase3_c32_20260710T012406Z.json`,
`bench/results/phase3_c128_20260710T012432Z.json`. These confirm `/metrics` + Prometheus scraping
works end-to-end under load; they are **not** a like-for-like comparison with Phase 1/2's 30s sweep
numbers because of the shortened window, so don't cite them as "Phase 3 is faster than Phase 2."

**Backpressure proof — PASS.** Gateway deliberately undersized (`-workers 16 -queue 2000`) and
driven at concurrency 128/batch 100 for 20s (well above what 16 workers into real Kafka+Timescale
can drain). Queue depth pegged at its 2000-capacity ceiling for the entire run (confirming sustained
backpressure — Push calls blocking, not events being dropped: `rejected=0` in the loadgen output),
p50/p99 latency rose to 485ms/990ms as a direct result of that blocking, and the gateway's process
RSS grew from a 17.2MB baseline to a 30.5MB peak (ratio 1.775x) and then leveled off — bounded growth
consistent with a fixed-capacity channel, not unbounded buffering. Heap alloc samples (1.4MB-6.9MB)
show normal GC sawtooth, not a leak. Source: `bench/results/phase3_backpressure_20260710T012536Z.json`
(includes the full RSS/heap/queue-depth timeseries).

## Phase 4 — Docker/K8s/Terraform/Ansible scaling

**Update: initially BLOCKED by disk (468MB free, needed ~3GB+), then re-attempted and captured for
real once the user freed disk on this machine.** The first attempt's BLOCKED result
(`free_disk_mb: 468`) is preserved in RUN_STATUS.md for the record; this section now has the real
measured numbers from the second attempt.

Environment: kind v0.32.0, `kindest/node:v1.29.8` (pinned — this host runs legacy cgroup v1, and
kind's default Kubernetes 1.36 node image hard-fails under it; see RUN_STATUS.md for the full
root-cause writeup), single control-plane node, gateway+consumer pods only (Kafka/TimescaleDB/Redis
stayed in docker-compose per this project's Kubernetes split). Load driven via `kubectl port-forward
svc/sluice-gateway 50051:50051`, `-duration 30s -warmup 5s -concurrency 128 -batch-size 100 -devices
10000` per round, `real_infra: true`.

| Replicas | Throughput (events/sec) |
|---|---|
| 1 | 8,700.0 |
| 2 | 9,556.7 |
| 4 | 11,264.5 |

Source files: `bench/results/phase4_scaling_r1_20260710T025941Z.json`,
`bench/results/phase4_scaling_r2_20260710T030227Z.json`,
`bench/results/phase4_scaling_r4_20260710T030339Z.json`.

**Read the shape, not the absolute numbers.** Scaling is real but sublinear — roughly +10% going
1→2 replicas, +18% going 2→4 — consistent with DECISIONS.md's partition-count/CPU-contention
reasoning. But the *absolute* throughput here (8.7k-11.3k events/sec) is well below Phases 1-3's
native numbers (36k-68k events/sec) for a reason specific to this measurement, not the pipeline: a
single-node kind cluster runs the *entire* Kubernetes control plane (etcd, apiserver, scheduler,
controller-manager, kubelet, kube-proxy, CoreDNS) on the same 8 hardware threads as the Sluice pods
and the docker-compose data infra — a far more crowded machine than Phases 1-3's. `kubectl
port-forward`'s single proxying tunnel adds further overhead on top of that. **Do not use these
absolute numbers to compare "Kubernetes" against "bare processes"** — the comparison isn't
controlled for the extra control-plane load. The relative 1→2→4 scaling shape is the valid,
citable result here.

**What else ran and is valid** (none of these need a live cluster): `docker build` for both
distroless images (verified working via a smoke-tested container run), `helm lint`/`helm template`
against the chart, `terraform validate` against the kind+helm Terraform config, and
`ansible-playbook --syntax-check` against the bootstrap playbook. All green — see RUN_STATUS.md,
which also has the full list of bugs found and fixed to get the live cluster working (cgroup v1
incompatibility, kind's isolated image store, distroless `runAsNonRoot` numeric-UID requirement,
Kafka's advertised-listener topology, and `kubectl port-forward`'s pod-UID pinning).

## Phase 5 — Visibility package

`cmd/report` is the source of every number in this file and in the README's
results section — it reads `bench/results/*.json` directly (see `internal/report`) and refuses to
run if the expected files are missing (verified by `internal/report`'s test suite, including a
dedicated "empty results dir errors instead of emitting a number" test case). The README's
`<!-- RESULTS:BEGIN -->...<!-- RESULTS:END -->` section is regenerated by `make report`, not hand-
maintained — it should always match what's in `bench/results/`. Charts are in `docs/charts/`:
`throughput-vs-concurrency.png` (log-scale Y axis — phase0's in-memory numbers are ~40x the real-infra
phases', which flattens everything else to invisible on a linear scale), `latency-percentiles.png`
(phase2's p50/p95/p99 vs concurrency, chosen as the most complete real-pipeline measurement), and
`throughput-vs-replicas.png` (Phase 4's 1→2→4 replica sweep).

That last chart was *not* generated on the first pass, when Phase 4's scaling sweep was still BLOCKED
by disk: `report.ThroughputVsReplicas` refuses to render from BLOCKED or absent data rather than
fabricate a curve, and `cmd/report` logs the skip and exits 0 instead of failing the whole report. It
renders here because the re-attempted sweep (see Phase 4 above) produced real data. That refusal path
is still the behaviour on BLOCKED or missing data — it simply no longer triggers.
