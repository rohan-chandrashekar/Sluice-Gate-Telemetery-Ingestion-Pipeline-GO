package report

import (
	"fmt"
	"strings"
)

const (
	MarkerBegin = "<!-- RESULTS:BEGIN -->"
	MarkerEnd   = "<!-- RESULTS:END -->"
)

func RenderMarkdown(rep *Report) string {
	var b strings.Builder

	b.WriteString(MarkerBegin + "\n\n")
	b.WriteString("Machine: " + rep.MachineTag + "\n\n")

	b.WriteString("### Ingest sweep (concurrency {8, 32, 128})\n\n")
	b.WriteString("| Phase | Sink | Real Infra | Concurrency | Throughput (events/sec) | p50 (ms) | p95 (ms) | p99 (ms) |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	for _, s := range rep.Sweeps {
		b.WriteString(fmt.Sprintf("| %s | %s | %v | %d | %s | %s | %s | %s |\n",
			s.Phase, s.Sink, s.RealInfra, s.Concurrency,
			formatFloat(s.ThroughputEventsPerSec), formatFloat(s.LatencyMsP50),
			formatFloat(s.LatencyMsP95), formatFloat(s.LatencyMsP99)))
	}
	b.WriteString("\n")

	b.WriteString("### Correctness\n\n")
	b.WriteString("| Test | Result |\n|---|---|\n")
	if rep.LossTest != nil {
		b.WriteString(fmt.Sprintf("| Kafka loss test (broker restart mid-produce) | %s (produced=%d, consumed=%d) |\n",
			passFail(rep.LossTest.Pass), rep.LossTest.Produced, rep.LossTest.Consumed))
	} else {
		b.WriteString("| Kafka loss test | — |\n")
	}
	if rep.Dedup != nil {
		b.WriteString(fmt.Sprintf("| Redis dedup (replay a batch twice) | %s (distinct_keys=%d, row_count=%d) |\n",
			passFail(rep.Dedup.Pass), rep.Dedup.DistinctKeys, rep.Dedup.RowCount))
	} else {
		b.WriteString("| Redis dedup | — |\n")
	}
	if rep.DLQ != nil {
		b.WriteString(fmt.Sprintf("| DLQ (malformed records + pipeline keeps flowing) | %s (injected=%d, in_dlq=%d, valid_consumed_after=%d) |\n",
			passFail(rep.DLQ.Pass), rep.DLQ.MalformedInjected, rep.DLQ.DLQCountObserved, rep.DLQ.ValidConsumedAfter))
	} else {
		b.WriteString("| DLQ | — |\n")
	}
	if rep.Backpressure != nil {
		b.WriteString(fmt.Sprintf("| Backpressure proof (RSS stays bounded under overload) | %s (ratio=%s, peak_queue_depth=%s) |\n",
			passFail(rep.Backpressure.Pass), rep.Backpressure.GrowthRatio, formatFloat(rep.Backpressure.PeakQueueDepth)))
	} else {
		b.WriteString("| Backpressure proof | — |\n")
	}
	b.WriteString("\n")

	b.WriteString("### Kubernetes replica scaling\n\n")
	if rep.Scaling == nil {
		b.WriteString("| Replicas | Throughput (events/sec) |\n|---|---|\n| — | — |\n")
	} else if rep.Scaling.Blocked {
		b.WriteString(fmt.Sprintf("BLOCKED: %s (%dMB free, %dMB required). See RUN_STATUS.md for the exact commands to run this on a machine with more disk.\n\n",
			rep.Scaling.BlockedInfo, rep.Scaling.FreeDiskMB, rep.Scaling.RequiredMB))
		b.WriteString("| Replicas | Throughput (events/sec) |\n|---|---|\n| 1 | — |\n| 2 | — |\n| 4 | — |\n")
	} else {
		b.WriteString("| Replicas | Throughput (events/sec) |\n|---|---|\n")
		for _, p := range rep.Scaling.Points {
			b.WriteString(fmt.Sprintf("| %d | %s |\n", p.Replicas, formatFloat(p.ThroughputEventsPerSec)))
		}
	}
	b.WriteString("\n")

	b.WriteString(MarkerEnd)
	return b.String()
}

func passFail(pass bool) string {
	if pass {
		return "PASS"
	}
	return "FAIL"
}

func formatFloat(f float64) string {
	return fmt.Sprintf("%.1f", f)
}

func ReplaceBetweenMarkers(original, replacement string) (string, error) {
	begin := strings.Index(original, MarkerBegin)
	end := strings.Index(original, MarkerEnd)
	if begin == -1 || end == -1 || end < begin {
		return "", fmt.Errorf("could not find %q / %q markers in target file", MarkerBegin, MarkerEnd)
	}
	end += len(MarkerEnd)
	return original[:begin] + replacement + original[end:], nil
}
