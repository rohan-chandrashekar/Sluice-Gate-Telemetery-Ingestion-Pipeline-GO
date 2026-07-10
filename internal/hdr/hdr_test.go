package hdr

import (
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

func TestMergeCombinesMultipleHistograms(t *testing.T) {
	h1 := New()
	h2 := New()

	for i := 0; i < 100; i++ {
		h1.RecordValue(int64(time.Millisecond))
	}
	for i := 0; i < 100; i++ {
		h2.RecordValue(int64(10 * time.Millisecond))
	}

	merged := Merge([]*hdrhistogram.Histogram{h1, h2})

	if got := merged.TotalCount(); got != 200 {
		t.Fatalf("expected 200 total samples after merge, got %d", got)
	}

	p := ComputePercentiles(merged)
	if p.P50Ms <= 0 || p.MaxMs < p.P50Ms {
		t.Fatalf("unexpected percentiles: %+v", p)
	}
}

func TestMergeSkipsNilHistograms(t *testing.T) {
	h1 := New()
	h1.RecordValue(int64(time.Millisecond))

	merged := Merge([]*hdrhistogram.Histogram{h1, nil})
	if got := merged.TotalCount(); got != 1 {
		t.Fatalf("expected 1 sample, got %d", got)
	}
}
