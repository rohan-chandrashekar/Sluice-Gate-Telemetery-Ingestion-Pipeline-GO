package hdr

import (
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const (
	lowestTrackableValue  = 1
	highestTrackableValue = int64(60 * time.Second)
	significantFigures    = 3
)

func New() *hdrhistogram.Histogram {
	return hdrhistogram.New(lowestTrackableValue, highestTrackableValue, significantFigures)
}

func Merge(hists []*hdrhistogram.Histogram) *hdrhistogram.Histogram {
	merged := New()
	for _, h := range hists {
		if h == nil {
			continue
		}
		merged.Merge(h)
	}
	return merged
}

type Percentiles struct {
	P50Ms float64
	P95Ms float64
	P99Ms float64
	MaxMs float64
}

func ComputePercentiles(h *hdrhistogram.Histogram) Percentiles {
	toMs := func(ns int64) float64 {
		return float64(ns) / float64(time.Millisecond)
	}
	return Percentiles{
		P50Ms: toMs(h.ValueAtQuantile(50)),
		P95Ms: toMs(h.ValueAtQuantile(95)),
		P99Ms: toMs(h.ValueAtQuantile(99)),
		MaxMs: toMs(h.Max()),
	}
}
