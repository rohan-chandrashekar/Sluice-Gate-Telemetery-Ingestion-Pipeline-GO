package results

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/benchmeta"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/hdr"
)

type Phase0Config struct {
	Target      string `json:"target"`
	Sink        string `json:"sink"`
	Workers     int    `json:"workers"`
	Queue       int    `json:"queue"`
	Duration    string `json:"duration"`
	Warmup      string `json:"warmup"`
	Concurrency int    `json:"concurrency"`
	BatchSize   int    `json:"batch_size"`
	Devices     int    `json:"devices"`
	RateHz      int    `json:"rate_hz"`
}

type LatencyMs struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

type Phase0Results struct {
	EventsAccepted         uint64    `json:"events_accepted"`
	EventsRejected         uint64    `json:"events_rejected"`
	Errors                 uint64    `json:"errors"`
	WindowSeconds          float64   `json:"window_seconds"`
	ThroughputEventsPerSec float64   `json:"throughput_events_per_sec"`
	LatencyMs              LatencyMs `json:"latency_ms"`
}

type Phase0File struct {
	Phase       string                `json:"phase"`
	Environment benchmeta.Environment `json:"environment"`
	Config      Phase0Config          `json:"config"`
	Results     Phase0Results         `json:"results"`
}

func LatencyFromPercentiles(p hdr.Percentiles) LatencyMs {
	return LatencyMs{P50: p.P50Ms, P95: p.P95Ms, P99: p.P99Ms, Max: p.MaxMs}
}

func WriteJSON(dir, prefix string, v any) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	ts := time.Now().UTC().Format("20060102T150405Z")
	path := filepath.Join(dir, fmt.Sprintf("%s_%s.json", prefix, ts))
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return "", err
	}
	return path, nil
}
