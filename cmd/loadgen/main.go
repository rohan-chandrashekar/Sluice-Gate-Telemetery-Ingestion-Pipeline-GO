package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/benchmeta"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/loadgen"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/results"
)

func main() {
	target := flag.String("target", "localhost:50051", "gateway grpc target")
	duration := flag.Duration("duration", 30*time.Second, "measurement window duration")
	warmup := flag.Duration("warmup", 5*time.Second, "warmup duration excluded from latency stats")
	concurrency := flag.Int("concurrency", 32, "number of concurrent client goroutines")
	batchSize := flag.Int("batch-size", 100, "events per Push call")
	devices := flag.Int("devices", 10000, "distinct device ids to simulate")
	rate := flag.Int("rate", 0, "target aggregate events/sec, 0 = unlimited")
	sinkLabel := flag.String("sink-label", "memory", "label recorded in results for which sink was under test")
	realInfra := flag.Bool("real-infra", false, "whether this run exercised real (non-mock) infra")
	resultsDir := flag.String("results-dir", "bench/results", "directory to write result JSON")
	resultsPrefix := flag.String("results-prefix", "phase0", "filename prefix for result JSON")
	gatewayWorkers := flag.Int("gateway-workers", 0, "gateway -workers value, recorded for context")
	gatewayQueue := flag.Int("gateway-queue", 0, "gateway -queue value, recorded for context")
	flag.Parse()

	cfg := loadgen.Config{
		Target:      *target,
		Duration:    *duration,
		Warmup:      *warmup,
		Concurrency: *concurrency,
		BatchSize:   *batchSize,
		Devices:     *devices,
		RateHz:      *rate,
	}

	log.Printf("loadgen starting target=%s concurrency=%d batch=%d duration=%s warmup=%s",
		cfg.Target, cfg.Concurrency, cfg.BatchSize, cfg.Duration, cfg.Warmup)

	res, err := loadgen.Run(context.Background(), cfg)
	if err != nil {
		log.Fatalf("loadgen run: %v", err)
	}

	log.Printf("accepted=%d rejected=%d errors=%d throughput=%.1f/s p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms",
		res.EventsAccepted, res.EventsRejected, res.Errors, res.ThroughputEventsPerSec,
		res.Latency.P50Ms, res.Latency.P95Ms, res.Latency.P99Ms, res.Latency.MaxMs)

	file := results.Phase0File{
		Phase:       *resultsPrefix,
		Environment: benchmeta.CurrentEnvironment(*realInfra),
		Config: results.Phase0Config{
			Target:      cfg.Target,
			Sink:        *sinkLabel,
			Workers:     *gatewayWorkers,
			Queue:       *gatewayQueue,
			Duration:    cfg.Duration.String(),
			Warmup:      cfg.Warmup.String(),
			Concurrency: cfg.Concurrency,
			BatchSize:   cfg.BatchSize,
			Devices:     cfg.Devices,
			RateHz:      cfg.RateHz,
		},
		Results: results.Phase0Results{
			EventsAccepted:         res.EventsAccepted,
			EventsRejected:         res.EventsRejected,
			Errors:                 res.Errors,
			WindowSeconds:          res.WindowSeconds,
			ThroughputEventsPerSec: res.ThroughputEventsPerSec,
			LatencyMs:              results.LatencyFromPercentiles(res.Latency),
		},
	}

	path, err := results.WriteJSON(*resultsDir, *resultsPrefix, file)
	if err != nil {
		log.Fatalf("write results: %v", err)
	}
	log.Printf("wrote %s", path)
}
