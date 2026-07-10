package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

type SweepPoint struct {
	Phase                  string
	Concurrency            int
	Sink                   string
	RealInfra              bool
	ThroughputEventsPerSec float64
	LatencyMsP50           float64
	LatencyMsP95           float64
	LatencyMsP99           float64
	LatencyMsMax           float64
	SourceFile             string
}

type LossTest struct {
	Produced int64
	Consumed int64
	Pass     bool
}

type DedupTest struct {
	DistinctKeys int64
	RowCount     int64
	Pass         bool
}

type DLQTest struct {
	MalformedInjected  int64
	DLQCountObserved   int64
	ValidConsumedAfter int64
	Pass               bool
}

type Backpressure struct {
	BaselineRSSBytes float64
	PeakRSSBytes     float64
	GrowthRatio      string
	PeakQueueDepth   float64
	Pass             bool
}

type ScalingPoint struct {
	Replicas               int
	ThroughputEventsPerSec float64
}

type Scaling struct {
	Blocked     bool
	BlockedInfo string
	FreeDiskMB  int64
	RequiredMB  int64
	Points      []ScalingPoint
}

type Report struct {
	Sweeps       []SweepPoint
	LossTest     *LossTest
	Dedup        *DedupTest
	DLQ          *DLQTest
	Backpressure *Backpressure
	Scaling      *Scaling
	MachineTag   string
}

var sweepFileRe = regexp.MustCompile(`^phase(\d+)_c(\d+)_\d{8}T\d{6}Z\.json$`)
var scalingRealFileRe = regexp.MustCompile(`^phase4_scaling_r(\d+)_\d{8}T\d{6}Z\.json$`)

type sweepFileShape struct {
	Environment struct {
		MachineTag string `json:"machine_tag"`
	} `json:"environment"`
	Config struct {
		Sink        string `json:"sink"`
		Concurrency int    `json:"concurrency"`
	} `json:"config"`
	Results struct {
		ThroughputEventsPerSec float64 `json:"throughput_events_per_sec"`
		LatencyMs              struct {
			P50 float64 `json:"p50"`
			P95 float64 `json:"p95"`
			P99 float64 `json:"p99"`
			Max float64 `json:"max"`
		} `json:"latency_ms"`
	} `json:"results"`
}

func Load(dir string) (*Report, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read results dir %s: %w", dir, err)
	}

	rep := &Report{}
	var sawAnySweep bool

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		path := filepath.Join(dir, name)

		switch {
		case sweepFileRe.MatchString(name):
			m := sweepFileRe.FindStringSubmatch(name)
			var shape sweepFileShape
			if err := readJSON(path, &shape); err != nil {
				return nil, err
			}
			realInfra := shape.Config.Sink != "memory"
			rep.Sweeps = append(rep.Sweeps, SweepPoint{
				Phase:                  "phase" + m[1],
				Concurrency:            shape.Config.Concurrency,
				Sink:                   shape.Config.Sink,
				RealInfra:              realInfra,
				ThroughputEventsPerSec: shape.Results.ThroughputEventsPerSec,
				LatencyMsP50:           shape.Results.LatencyMs.P50,
				LatencyMsP95:           shape.Results.LatencyMs.P95,
				LatencyMsP99:           shape.Results.LatencyMs.P99,
				LatencyMsMax:           shape.Results.LatencyMs.Max,
				SourceFile:             name,
			})
			sawAnySweep = true
			if rep.MachineTag == "" {
				rep.MachineTag = shape.Environment.MachineTag
			}

		case matchPrefix(name, "phase1_losstest_"):
			var shape struct {
				Produced   int64  `json:"produced"`
				Consumed   int64  `json:"consumed"`
				Pass       bool   `json:"pass"`
				MachineTag string `json:"machine_tag"`
			}
			if err := readJSON(path, &shape); err != nil {
				return nil, err
			}
			rep.LossTest = &LossTest{Produced: shape.Produced, Consumed: shape.Consumed, Pass: shape.Pass}
			if rep.MachineTag == "" {
				rep.MachineTag = shape.MachineTag
			}

		case matchPrefix(name, "phase2_correctness_"):
			var shape struct {
				DedupTest struct {
					DistinctKeys int64 `json:"distinct_keys"`
					RowCount     int64 `json:"row_count"`
					Pass         bool  `json:"pass"`
				} `json:"dedup_test"`
				DLQTest struct {
					MalformedInjected  int64 `json:"malformed_injected"`
					DLQCountObserved   int64 `json:"dlq_count_observed"`
					ValidConsumedAfter int64 `json:"valid_events_consumed_after_injection"`
					Pass               bool  `json:"pass"`
				} `json:"dlq_test"`
				MachineTag string `json:"machine_tag"`
			}
			if err := readJSON(path, &shape); err != nil {
				return nil, err
			}
			rep.Dedup = &DedupTest{
				DistinctKeys: shape.DedupTest.DistinctKeys,
				RowCount:     shape.DedupTest.RowCount,
				Pass:         shape.DedupTest.Pass,
			}
			rep.DLQ = &DLQTest{
				MalformedInjected:  shape.DLQTest.MalformedInjected,
				DLQCountObserved:   shape.DLQTest.DLQCountObserved,
				ValidConsumedAfter: shape.DLQTest.ValidConsumedAfter,
				Pass:               shape.DLQTest.Pass,
			}
			if rep.MachineTag == "" {
				rep.MachineTag = shape.MachineTag
			}

		case matchPrefix(name, "phase3_backpressure_") && !matchPrefix(name, "phase3_backpressure_load_"):
			var shape struct {
				BaselineRSSBytes float64 `json:"baseline_rss_bytes"`
				PeakRSSBytes     float64 `json:"peak_rss_bytes"`
				RSSGrowthRatio   string  `json:"rss_growth_ratio"`
				PeakQueueDepth   float64 `json:"peak_queue_depth"`
				Pass             bool    `json:"pass"`
				MachineTag       string  `json:"machine_tag"`
			}
			if err := readJSON(path, &shape); err != nil {
				return nil, err
			}
			rep.Backpressure = &Backpressure{
				BaselineRSSBytes: shape.BaselineRSSBytes,
				PeakRSSBytes:     shape.PeakRSSBytes,
				GrowthRatio:      shape.RSSGrowthRatio,
				PeakQueueDepth:   shape.PeakQueueDepth,
				Pass:             shape.Pass,
			}
			if rep.MachineTag == "" {
				rep.MachineTag = shape.MachineTag
			}

		case matchPrefix(name, "phase4_scaling_") && matchSuffix(name, "_BLOCKED.json"):
			var shape struct {
				Status     string `json:"status"`
				Reason     string `json:"reason"`
				FreeDiskMB int64  `json:"free_disk_mb"`
				RequiredMB int64  `json:"required_disk_mb"`
				MachineTag string `json:"machine_tag"`
			}
			if err := readJSON(path, &shape); err != nil {
				return nil, err
			}
			rep.Scaling = &Scaling{
				Blocked:     true,
				BlockedInfo: shape.Reason,
				FreeDiskMB:  shape.FreeDiskMB,
				RequiredMB:  shape.RequiredMB,
			}
			if rep.MachineTag == "" {
				rep.MachineTag = shape.MachineTag
			}

		case scalingRealFileRe.MatchString(name):
			m := scalingRealFileRe.FindStringSubmatch(name)
			var shape sweepFileShape
			if err := readJSON(path, &shape); err != nil {
				return nil, err
			}
			if rep.Scaling == nil {
				rep.Scaling = &Scaling{}
			}
			replicas, _ := strconv.Atoi(m[1])
			rep.Scaling.Points = append(rep.Scaling.Points, ScalingPoint{
				Replicas:               replicas,
				ThroughputEventsPerSec: shape.Results.ThroughputEventsPerSec,
			})
		}
	}

	if !sawAnySweep {
		return nil, fmt.Errorf("no phase0 sweep results found in %s; refusing to render a results table from nothing - run scripts/run_phase0.sh first", dir)
	}

	sort.Slice(rep.Sweeps, func(i, j int) bool {
		if rep.Sweeps[i].Phase != rep.Sweeps[j].Phase {
			return rep.Sweeps[i].Phase < rep.Sweeps[j].Phase
		}
		return rep.Sweeps[i].Concurrency < rep.Sweeps[j].Concurrency
	})
	if rep.Scaling != nil {
		sort.Slice(rep.Scaling.Points, func(i, j int) bool {
			return rep.Scaling.Points[i].Replicas < rep.Scaling.Points[j].Replicas
		})
	}

	return rep, nil
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

func matchPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func matchSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
