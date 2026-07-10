package report

import (
	"strings"
	"testing"
)

func TestLoadFixtures(t *testing.T) {
	rep, err := Load("../../testdata/results")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(rep.Sweeps) != 2 {
		t.Fatalf("expected 2 sweep points, got %d", len(rep.Sweeps))
	}
	if rep.Sweeps[0].Concurrency != 8 || rep.Sweeps[1].Concurrency != 32 {
		t.Fatalf("unexpected sweep ordering: %+v", rep.Sweeps)
	}
	if rep.Sweeps[0].ThroughputEventsPerSec != 100000.0 {
		t.Fatalf("unexpected throughput: %+v", rep.Sweeps[0])
	}

	if rep.LossTest == nil || !rep.LossTest.Pass || rep.LossTest.Produced != 1000 {
		t.Fatalf("unexpected loss test: %+v", rep.LossTest)
	}

	if rep.Scaling == nil || !rep.Scaling.Blocked {
		t.Fatalf("expected scaling to be blocked, got: %+v", rep.Scaling)
	}
	if rep.Scaling.FreeDiskMB != 468 {
		t.Fatalf("unexpected free disk mb: %+v", rep.Scaling)
	}

	if rep.MachineTag != "test-machine" {
		t.Fatalf("unexpected machine tag: %q", rep.MachineTag)
	}
}

func TestLoadErrorsOnMissingSweepData(t *testing.T) {
	_, err := Load("../../testdata/empty")
	if err == nil {
		t.Fatal("expected an error when no phase0 sweep results are present, got nil")
	}
	if !strings.Contains(err.Error(), "no phase0 sweep results found") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestLoadErrorsOnMissingDirectory(t *testing.T) {
	_, err := Load("../../testdata/does-not-exist")
	if err == nil {
		t.Fatal("expected an error for a nonexistent directory, got nil")
	}
}

func TestRenderMarkdownIncludesSweepAndCorrectness(t *testing.T) {
	rep, err := Load("../../testdata/results")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	md := RenderMarkdown(rep)

	for _, want := range []string{
		MarkerBegin,
		MarkerEnd,
		"phase0",
		"100000.0",
		"PASS (produced=1000, consumed=1000)",
		"BLOCKED: insufficient disk for kind node image",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("rendered markdown missing %q\n---\n%s", want, md)
		}
	}
}

func TestReplaceBetweenMarkersRoundTrips(t *testing.T) {
	original := "# Title\n\n" + MarkerBegin + "\nold content\n" + MarkerEnd + "\n\nfooter\n"
	updated, err := ReplaceBetweenMarkers(original, MarkerBegin+"\nnew content\n"+MarkerEnd)
	if err != nil {
		t.Fatalf("ReplaceBetweenMarkers: %v", err)
	}
	if strings.Contains(updated, "old content") {
		t.Fatal("old content should have been replaced")
	}
	if !strings.Contains(updated, "new content") || !strings.Contains(updated, "footer") {
		t.Fatalf("unexpected result: %s", updated)
	}
}

func TestReplaceBetweenMarkersErrorsWhenMarkersMissing(t *testing.T) {
	_, err := ReplaceBetweenMarkers("no markers here", "replacement")
	if err == nil {
		t.Fatal("expected an error when markers are missing")
	}
}

func TestHeadlinesPicksPeakThroughputAndFlagsBlockedScaling(t *testing.T) {
	rep, err := Load("../../testdata/results")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	h := rep.Headlines()
	if h.PeakThroughputEventsPerSec != 200000.0 {
		t.Fatalf("expected peak throughput 200000, got %v", h.PeakThroughputEventsPerSec)
	}
	if h.PeakThroughputConcurrency != 32 {
		t.Fatalf("expected peak concurrency 32, got %d", h.PeakThroughputConcurrency)
	}
	if !h.ZeroLossConfirmed {
		t.Fatal("expected zero loss confirmed from fixture loss test")
	}
	if !strings.Contains(h.ScalingFactor, "BLOCKED") {
		t.Fatalf("expected scaling factor to mention BLOCKED, got %q", h.ScalingFactor)
	}
}

func TestChartsSkipGracefullyWhenScalingDataMissing(t *testing.T) {
	rep, err := Load("../../testdata/results")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	tmp := t.TempDir()
	if _, err := ThroughputVsReplicas(rep, tmp); err == nil {
		t.Fatal("expected an error rendering throughput-vs-replicas with only BLOCKED phase4 data")
	}
}
