package report

import "fmt"

type Headlines struct {
	PeakThroughputEventsPerSec float64
	PeakThroughputPhase        string
	PeakThroughputConcurrency  int
	P99AtPeakMs                float64

	RealInfraPeakThroughputEventsPerSec float64
	RealInfraPeakPhase                  string
	RealInfraPeakConcurrency            int
	RealInfraP99AtPeakMs                float64

	ZeroLossConfirmed bool
	DedupConfirmed    bool
	ScalingFactor     string
	MachineTag        string
}

func (r *Report) Headlines() Headlines {
	h := Headlines{MachineTag: r.MachineTag}

	for _, s := range r.Sweeps {
		if s.ThroughputEventsPerSec > h.PeakThroughputEventsPerSec {
			h.PeakThroughputEventsPerSec = s.ThroughputEventsPerSec
			h.PeakThroughputPhase = s.Phase
			h.PeakThroughputConcurrency = s.Concurrency
			h.P99AtPeakMs = s.LatencyMsP99
		}
		if s.RealInfra && s.ThroughputEventsPerSec > h.RealInfraPeakThroughputEventsPerSec {
			h.RealInfraPeakThroughputEventsPerSec = s.ThroughputEventsPerSec
			h.RealInfraPeakPhase = s.Phase
			h.RealInfraPeakConcurrency = s.Concurrency
			h.RealInfraP99AtPeakMs = s.LatencyMsP99
		}
	}

	if r.LossTest != nil {
		h.ZeroLossConfirmed = r.LossTest.Pass && r.LossTest.Produced == r.LossTest.Consumed
	}

	if r.Dedup != nil {
		h.DedupConfirmed = r.Dedup.Pass
	}

	if r.Scaling != nil && !r.Scaling.Blocked && len(r.Scaling.Points) >= 2 {
		first := r.Scaling.Points[0]
		last := r.Scaling.Points[len(r.Scaling.Points)-1]
		if first.ThroughputEventsPerSec > 0 {
			factor := last.ThroughputEventsPerSec / first.ThroughputEventsPerSec
			h.ScalingFactor = fmt.Sprintf("%.2fx (%d -> %d replicas)", factor, first.Replicas, last.Replicas)
		}
	} else if r.Scaling != nil && r.Scaling.Blocked {
		h.ScalingFactor = "not measured on this machine (BLOCKED - see RUN_STATUS.md)"
	}

	return h
}
