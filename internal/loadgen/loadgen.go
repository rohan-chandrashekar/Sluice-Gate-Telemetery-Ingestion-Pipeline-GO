package loadgen

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/HdrHistogram/hdrhistogram-go"

	sluicev1 "github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/gen/sluice/v1"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/hdr"
)

type Config struct {
	Target      string
	Duration    time.Duration
	Warmup      time.Duration
	Concurrency int
	BatchSize   int
	Devices     int
	RateHz      int
}

type Result struct {
	EventsAccepted         uint64
	EventsRejected         uint64
	Errors                 uint64
	WindowSeconds          float64
	ThroughputEventsPerSec float64
	Latency                hdr.Percentiles
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	conn, err := grpc.NewClient(cfg.Target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return Result{}, fmt.Errorf("dial %s: %w", cfg.Target, err)
	}
	defer conn.Close()

	client := sluicev1.NewIngestClient(conn)

	var accepted, rejected, errs uint64
	hists := make([]*hdrhistogram.Histogram, cfg.Concurrency)

	runCtx, cancel := context.WithTimeout(ctx, cfg.Warmup+cfg.Duration)
	defer cancel()

	start := time.Now()
	recordAfter := start.Add(cfg.Warmup)

	var wg sync.WaitGroup
	wg.Add(cfg.Concurrency)
	for g := 0; g < cfg.Concurrency; g++ {
		go func(id int) {
			defer wg.Done()
			h := hdr.New()
			hists[id] = h

			var seq uint64
			var ticker *time.Ticker
			if cfg.RateHz > 0 {
				perGoroutineHz := float64(cfg.RateHz) / float64(cfg.Concurrency)
				if perGoroutineHz <= 0 {
					perGoroutineHz = 1
				}
				interval := time.Duration(float64(time.Second) / perGoroutineHz)
				ticker = time.NewTicker(interval)
				defer ticker.Stop()
			}

			for {
				select {
				case <-runCtx.Done():
					return
				default:
				}

				if ticker != nil {
					select {
					case <-runCtx.Done():
						return
					case <-ticker.C:
					}
				}

				req := &sluicev1.IngestRequest{Events: make([]*sluicev1.TelemetryEvent, cfg.BatchSize)}
				now := time.Now().UnixNano()
				for i := 0; i < cfg.BatchSize; i++ {
					seq++
					deviceN := seq % uint64(max(cfg.Devices, 1))
					req.Events[i] = &sluicev1.TelemetryEvent{
						DeviceId:           fmt.Sprintf("device-%d", deviceN),
						IdempotencyKey:     fmt.Sprintf("g%d-%d", id, seq),
						EventTimeUnixNanos: now,
						Metric:             "cpu.util",
						Value:              float64(seq % 100),
					}
				}

				callStart := time.Now()
				resp, err := client.Push(runCtx, req)
				elapsed := time.Since(callStart)

				if err != nil {
					atomic.AddUint64(&errs, 1)
					continue
				}

				atomic.AddUint64(&accepted, resp.GetAccepted())
				atomic.AddUint64(&rejected, resp.GetRejected())

				if time.Now().After(recordAfter) {
					h.RecordValue(elapsed.Nanoseconds())
				}
			}
		}(g)
	}
	wg.Wait()

	windowSeconds := cfg.Duration.Seconds()
	merged := hdr.Merge(hists)

	return Result{
		EventsAccepted:         accepted,
		EventsRejected:         rejected,
		Errors:                 errs,
		WindowSeconds:          windowSeconds,
		ThroughputEventsPerSec: float64(accepted) / windowSeconds,
		Latency:                hdr.ComputePercentiles(merged),
	}, nil
}
