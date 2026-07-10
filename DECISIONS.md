# DECISIONS

Design rationale, accumulated per phase. This is the "why", not the "what" — the code and
RUN_STATUS.md cover the what.

## Phase 0

**Bounded pool + blocking backpressure, never drop.** The gateway's worker pool is a fixed-size
buffered `chan sink.Event` (`-queue`) drained by a fixed number of goroutines (`-workers`).
`Submit` does a blocking channel send select'd against `ctx.Done()`. This means a slow or stalled
sink (Kafka down, Timescale down) applies backpressure all the way back to the gRPC caller instead
of silently dropping telemetry or growing memory unboundedly. The cost is that a stalled sink turns
into blocked `Push` calls — which is exactly the signal an operator wants (visible in p99 latency
and queue-depth metrics in Phase 3) instead of a silent gap in the data.

**Why a shared bounded channel instead of one goroutine per request.** Unbounded goroutine-per-event
fan-out under sustained overload would exhaust memory before it exhausted CPU, defeating the
16GB-RAM constraint on this exact machine. A fixed pool caps concurrent sink work at `-workers`
regardless of inbound request rate.

**Why `Sink` is a one-method interface.** `Write(ctx, Event) error` is the only contract the pool,
the gateway, and the consumer need to agree on. Every phase after this one only adds new
implementations of it (`KafkaSink`, `TimescaleSink`) — never changes the pool or the gateway's
RPC handler. This is why Phase 1's `KafkaSink` and Phase 2's `TimescaleSink` slot in without
touching `internal/pool` or `internal/gateway`.

**Why HDR histograms per-goroutine, merged at the end, instead of a shared histogram.**
`hdrhistogram.Histogram` isn't safe for concurrent writes. Giving each loadgen goroutine its own
histogram avoids a mutex on the hot path, at the cost of an O(goroutines) merge once at the end —
negligible compared to a 30s run.

**Why franz-go over confluent-kafka-go / sarama's CGO variants (applies starting Phase 1).**
Fixed per the brief: `github.com/twmb/franz-go`, no CGO — avoids the CGO+librdkafka build/runtime
dependency entirely, which matters on a disk-constrained machine (no extra apt packages needed) and
keeps cross-compilation for the distroless Phase 4 image simple.

## Phase 1

**Partition by device_id.** Keying every Kafka record by `device_id` guarantees all events for a
given device land on the same partition and are therefore processed in order by the same consumer
within a group — important for any downstream logic that assumes per-device ordering (e.g. a
later rollup or anomaly detector), without needing a single global ordering across all devices.

**Idempotent producer (acks=all) over fire-and-forget.** `acks=all` plus franz-go's default
idempotent-producer behavior means a produce either durably lands on all in-sync replicas or the
call fails — paired with the gateway's blocking backpressure, this keeps "never drop" true all the
way from the gRPC call to disk on the broker, at the direct cost of per-event produce latency (see
RESULTS.md: Phase 1 throughput is ~40-60x lower than Phase 0's in-memory ceiling). That tradeoff is
deliberate: this project optimizes for zero silent data loss over raw throughput.

**Commit-after-write (at-least-once), not commit-before or auto-commit.** The consumer calls
`CommitUncommittedOffsets` only after every record in a fetched batch has been written to the sink
successfully. If the consumer crashes between writing and committing, the same records get
reprocessed on restart — at-least-once, never at-most-once. This is why Phase 2 needs a dedup layer
(Redis `SETNX`) to get to effectively-once at the sink.

**Single KRaft broker, no ZooKeeper.** `apache/kafka:3.7.1`'s built-in KRaft combined mode (broker
+ controller in one process) avoids running a second ZooKeeper container entirely — meaningful on a
16GB-RAM, disk-constrained (down to ~2.4-3GB free at points in this session) machine where every
extra container is real budget. Modest heap (`-Xmx512m -Xms512m`) and a 900MB container memory cap
keep it from competing with the rest of the stack.

**franz-go client used concurrently across pool workers instead of one client per worker.** A
single `*kgo.Client` is safe for concurrent `Produce`/`ProduceSync` calls, so `KafkaSink` holds one
client shared by every worker goroutine in the pool — avoids N separate TCP connections and
producer sessions for N workers, letting the broker-side idempotent-producer sequencing work over
one producer ID.

## Phase 2

**Batched CopyFrom with a whole-batch-then-per-row fallback, not per-event INSERT.** `TimescaleSink`
buffers events and flushes via `pgx`'s `CopyFrom` on `-batch` size or `-flush-interval`, whichever
comes first — one round trip for hundreds of rows instead of one per row. If a `CopyFrom` batch
fails, it's retried whole (`-max-retries` times) before falling back to per-row `INSERT ... ON
CONFLICT DO NOTHING` with its own per-row retry budget. This two-tier retry deliberately
distinguishes a transient outage (the whole batch retry resolves it cheaply) from a genuine poison
row (only the per-row fallback isolates *which* row is bad, so one malformed value can't block an
entire otherwise-healthy batch — `CopyFrom`/`COPY` is all-or-nothing per statement, so without this
fallback a single bad row would repeatedly fail the whole batch forever).

**Flush-then-commit, not commit-per-record.** The `Sink` interface stays a single `Write` method
(per the stable-contract rule), but `TimescaleSink` additionally implements an optional `Flusher`
(`Flush(ctx) error`) and `PoisonReporter` (`DrainPoisoned() []PoisonedEvent`) interface that the
consumer type-asserts for. The consumer processes one whole `PollFetches` batch (decode → batch
dedup → persist for every record), then calls `Flush` if the sink supports it, and only commits
offsets if that flush succeeds. If `Flush` fails, the round's offsets are simply not committed —
the buffered-but-unflushed events stay safely in `TimescaleSink`'s in-memory buffer for the next
flush attempt (background ticker or next round), and a process crash before a successful flush
means the same records get reprocessed after restart from the last real commit. This is what makes
"commit only offsets covered by a completed flush" true without needing per-record acknowledgment
from a batched sink.

**Pipelined Redis dedup, not one `SETNX` per event — found the hard way.** The first implementation
called `Deduper.Seen` once per event, synchronously, inside the consumer's per-record loop. Under
real load this capped consumption at roughly 100-500 events/sec (network round-trip bound) and,
combined with a since-fixed sweep design that let one topic accumulate a multi-million-record
backlog across rounds, produced a consumer that could never catch up — see RUN_STATUS.md for the
full incident. Fixed by adding `Deduper.SeenBatch`, which pipelines every key in a fetched batch
into one Redis round trip, and restructuring the consumer around `handleBatch` (decode the whole
batch, one pipelined dedup check for all of it, then persist each non-duplicate) instead of a
per-record callback. Lesson generalized: anything that does synchronous network I/O per event
inside a hot consumer loop needs to be batched from the start, not retrofitted after a sweep proves
it can't keep up.

**One Kafka topic per sweep round, not one shared topic across rounds.** Reusing a topic across
concurrency rounds means every round's fresh consumer group (no prior committed offset) replays the
*entire* cumulative history from offset 0 by franz-go's default "earliest" reset — Phase 1 got away
with this because `MemorySink` could blow through a multi-million-record replay in seconds; Phase 2
could not, for the Redis-latency reason above. Giving each round its own topic
(`telemetry.events.c8`/`.c32`/`.c128`) makes every round measure only its own traffic and removes
the unbounded-replay failure mode entirely.

**Dead-letter on unmarshal failure is immediate; dead-letter on persist failure is retried first.**
A record that fails `proto.Unmarshal` will never parse no matter how many times it's retried, so it
goes straight to the DLQ with the raw bytes preserved. A record that fails to persist might be
failing for a transient reason (a lock, a momentary connection blip), so it gets `-max-retries`
attempts (at the row level, inside `TimescaleSink`'s fallback path) before being marked poisoned and
forwarded to the DLQ — as a re-marshaled `TelemetryEvent` rather than byte-identical original bytes,
since by the time a row is identified as poisoned it has already been decoded and separated from its
original Kafka record inside a batch; this is a deliberate simplification (documented here rather
than silently) since the DLQ's purpose is operational visibility and replay, not bit-perfect
archival.

## Phase 3

**Block-vs-shed as an explicit, opt-in mode, not a replacement for the default.** `-shed` makes
`Push` use `TrySubmit` (drop-if-full) instead of the default blocking `Submit`. The default stays
blocking because this project's whole premise is "never silently drop telemetry" — but a real
operator sometimes needs the opposite tradeoff (e.g. serving best-effort dashboards where stale data
beats a stalled upstream), so it's offered as an explicit, metric-instrumented (`sluice_gateway_shed_total`)
choice rather than baked in silently.

**Metrics as an optional collaborator, not a hard dependency of the hot path.** `internal/gateway`
and `internal/consumer` accept a `*metrics.Gateway`/`*metrics.Consumer` that can be `nil`, and every
call site nil-checks before touching it. This keeps `internal/gateway`'s and `internal/consumer`'s
existing offline unit tests working unchanged (they pass `nil`) while production wiring in `cmd/`
always constructs real collectors — avoids a hard Prometheus dependency bleeding into code that
should stay testable without any exporter running.

**Queue depth as a `GaugeFunc` sampled on scrape, not a value pushed on every enqueue/dequeue.**
`prometheus.NewGaugeFunc` calls `pool.QueueDepth()` (a plain `len(chan)`) only when Prometheus
scrapes `/metrics`, instead of updating a gauge on every `Submit`/dequeue — zero overhead on the hot
path, and `len()` on a channel is already O(1) and always current, so there's nothing to gain from
maintaining it incrementally.

**Disk, not RAM, turned out to be this machine's real binding constraint for Phase 3 — found the
hard way, twice.** The brief anticipated RAM as the tight constraint on this laptop; in practice the
root disk started this session at 96% full with only ~2-3GB free, and Phase 3's heaviest stack
(Kafka + Redis + TimescaleDB + Prometheus + Grafana, all five containers, plus three real sweep
rounds and a sustained-overload backpressure test) drove free space to *exactly* zero twice before
the scripts were fixed to actively manage disk instead of assuming it would be fine — see
RUN_STATUS.md for the full incident writeups. Two lessons generalized into the scripts from here on:
(1) any script that lets multiple rounds of real load write into the same long-lived Kafka/Timescale
volumes must clean up between rounds (topic delete + table truncate, with Kafka's
`log.segment.delete.delay.ms` turned down from its 60s default so deletes actually reclaim disk
before the next round needs it) — accumulation across "harmless" repeated rounds is exactly what
filled the disk, not any single round in isolation; (2) a `df`-based preflight check before any
disk-hungry step, that aborts with a clear BLOCKED message and the exact rerun command, is cheap
insurance against ever hitting this blind again — a full disk breaks the shell itself, not just the
running script, which is a far worse failure mode than a clean early exit.

## Phase 4

**Only Sluice's own pods run inside kind; Kafka/TimescaleDB/Redis stay in docker-compose on the
host.** Running the full data-infra stack a second time inside a kind cluster, on top of
docker-compose, would roughly double the container/memory/disk footprint for no benefit — the point
of Phase 4 is to prove Sluice's own deployment story (Helm chart, HPA, resource limits, Terraform
IaC), not to re-host Kafka in Kubernetes too. Pods reach the host-published Kafka/Timescale/Redis
ports via the kind bridge network's gateway IP rather than a Kubernetes Service, because those
services genuinely aren't Kubernetes-managed in this topology — pretending otherwise (e.g. an
ExternalName Service) would be more indirection for no correctness benefit.

**`tehcyx/kind` + `hashicorp/helm` Terraform providers, one `terraform apply` for cluster and
release.** Keeping cluster creation and the application release in the same Terraform state means
`consumer_replicas` becomes a single `-var` away from a re-apply for the scaling sweep, and
`terraform destroy` tears down cleanly — important on a disk-constrained machine where leftover kind
clusters are exactly the kind of thing that quietly eats the remaining headroom between runs.

**HPA ships CPU-based by default; lag-based scaling is wired in but off.** Native Kubernetes HPA
only understands `Resource` metrics (CPU/memory) out of the box. Scaling on `sluice_consumer_group_lag`
(a business metric this project's consumer already exports) requires a Prometheus Adapter or KEDA
translating that into the custom/external metrics API — cluster-wide infrastructure, not something
an application's own Helm chart should install as a side effect. The chart's `hpa.lagBased.enabled`
flag and the metric name are already there for whoever installs that adapter; CPU-based scaling is
the default because it works with zero extra cluster setup.

**Why the scaling curve flattens early on this machine — anticipated here, then confirmed by a real
measurement after a second pass unblocked the disk constraint (1→8,700, 2→9,556.7, 4→11,264.5
events/sec; see RESULTS.md).** The measured shape (+10% then +18%, never doubling) matches this
reasoning, written before the real numbers existed. Three things cap how much throughput adding
consumer replicas can buy on this specific 8-thread machine, in order of how binding each becomes:
1. **Partition count as the hard parallelism ceiling.** A Kafka consumer group can never have more
   *active* consumers than partitions on a topic — with `-partitions 12` (this project's default),
   replica count 12 is already the point beyond which additional consumer pods sit idle with no
   partitions assigned, regardless of CPU headroom. The Helm chart's `hpa.maxReplicas: 8` stays
   under that ceiling deliberately.
2. **CPU-bound past N consumers on 8 hardware threads.** Every consumer replica runs its own decode
   → dedup-pipeline-RPC → batched-CopyFrom loop, each needing real CPU for protobuf unmarshaling and
   Redis/Postgres round trips. Once the sum of active consumers' CPU demand approaches the 8 threads
   available, adding another replica doesn't add parallel *capacity* so much as more schedulable work
   competing for the same cores — a classic throughput-vs-replicas curve that rises steeply then
   flattens, not a straight line.
3. **Context-switch and connection overhead compound as replica count grows.** Each replica holds
   its own Kafka client, Postgres pool connections, and Redis client; more replicas means more
   concurrent connections and more OS-level context switching for the same total useful work, which
   eats into the marginal benefit of each additional replica faster than a naive N-way linear
   scaling model would predict.

None of this is a bug — it is the honest ceiling of an 8-thread machine with data infra co-resident
with the workload, and precisely the kind of result this project's Numbers Discipline insists on
reporting plainly rather than dressing up.

**A fourth factor, specific to the real measurement and not present in Phases 1-3: the full
Kubernetes control plane shares the same 8 threads too.** A single-node kind cluster runs etcd,
kube-apiserver, the scheduler, controller-manager, kubelet, kube-proxy, and CoreDNS as real
workloads on the same machine as the Sluice pods — Phases 1-3 never had this overhead, since there
was no orchestrator, just the gateway/consumer processes and the data infra. This is the main reason
Phase 4's absolute throughput (8.7k-11.3k events/sec) reads much lower than Phase 1-3's
(36k-68k events/sec): it is not a regression in the pipeline, it is a more crowded machine. Combined
with `kubectl port-forward`'s single-tunnel proxying overhead (used to reach the gateway Service from
the load generator), the absolute numbers from this measurement should never be cited as "Kubernetes
overhead vs. bare-metal Sluice" — only the relative 1→2→4 replica shape is a controlled comparison
here, since every round paid the identical control-plane and port-forward tax.

**Two Kubernetes-specific pitfalls worth recording as decisions, not just incidents (full writeup in
RUN_STATUS.md):** distroless's symbolic `USER nonroot` needs an explicit numeric `runAsUser: 65532`
alongside `runAsNonRoot: true` in the pod spec — Kubernetes won't resolve a non-numeric username
against the image's own passwd file to satisfy that check, so `runAsNonRoot: true` alone silently
fails every pod. And this host's legacy cgroup v1 hierarchy — confirmed via `stat -fc %T
/sys/fs/cgroup/` and a hybrid mount layout with no `systemd.unified_cgroup_hierarchy=1` boot
parameter — is incompatible with kind's default (Kubernetes 1.31+) node images; pinning
`kindest/node:v1.29.8` via a new `kind_node_image` Terraform variable was the fix chosen over
reconfiguring the host's cgroup hierarchy, since the latter needs a reboot and is a much larger,
less reversible change for a live session to make unilaterally.

## Phase 5

**A single JSON schema (`internal/results.Phase0File`) reused by every phase's loadgen output,
instead of a bespoke schema per phase.** Because `cmd/loadgen` writes the same shape regardless of
which phase invokes it, `internal/report`'s loader can parse every `phaseN_cM_<ts>.json` sweep file
with one struct and one filename regex — phase-specific correctness tests (loss test, dedup/DLQ,
backpressure) each get their own small ad-hoc shape instead, because they genuinely aren't sweep
data and forcing them into the sweep shape would be the wrong kind of consistency.

**The loader fails loudly on missing sweep data, but tolerates every other file being absent.**
Phase0 sweep results are treated as the one hard requirement (`Load` errors if none are found) since
a results table with zero throughput numbers isn't a report, it's a placeholder pretending to be one.
Everything else — loss test, dedup, DLQ, backpressure, scaling — is optional and rendered as an
explicit `—` or a passed-through BLOCKED message when absent, matching the brief's requirement to
leave NOT-VALID/BLOCKED cells as placeholders rather than invent or omit them silently.

**Log-scale Y axis on the throughput-vs-concurrency chart, chosen after looking at the linear-scale
version first.** Phase 0's in-memory ceiling (~1.8M-2.5M events/sec) is roughly 40x every real-infra
phase's numbers (25k-70k events/sec) — on a linear axis, phases 1-3 render as a flat line pinned to
zero, which actively misleads a reader into thinking real-infra throughput doesn't vary with
concurrency at all. This is disclosed here rather than left as a silent formatting choice because it
changes what the chart visually claims.

**Chart generation fails independently per chart, not as one atomic all-or-nothing step.**
`cmd/report` logs and continues if one chart's prerequisite data is missing (e.g.
throughput-vs-replicas with BLOCKED phase4 data) instead of aborting the whole report run — the
README table and the other two charts are still valid and worth producing even when Phase 4's
cluster-based measurement couldn't happen on this machine.
