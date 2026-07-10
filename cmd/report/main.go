package main

import (
	"flag"
	"log"
	"os"

	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/report"
)

func main() {
	resultsDir := flag.String("results-dir", "bench/results", "directory containing bench result JSON files")
	readmePath := flag.String("readme", "README.md", "README file to update between the results markers")
	chartsDir := flag.String("charts-dir", "docs/charts", "directory to write chart PNGs into")
	latencyPhase := flag.String("latency-phase", "phase2", "which sweep phase's latency percentiles to chart")
	flag.Parse()

	rep, err := report.Load(*resultsDir)
	if err != nil {
		log.Fatalf("report: %v", err)
	}

	rendered := report.RenderMarkdown(rep)

	original, err := os.ReadFile(*readmePath)
	if err != nil {
		log.Fatalf("read %s: %v", *readmePath, err)
	}

	updated, err := report.ReplaceBetweenMarkers(string(original), rendered)
	if err != nil {
		log.Fatalf("update README: %v", err)
	}
	if err := os.WriteFile(*readmePath, []byte(updated), 0o644); err != nil {
		log.Fatalf("write %s: %v", *readmePath, err)
	}
	log.Printf("updated results table in %s", *readmePath)

	if path, err := report.ThroughputVsConcurrency(rep, *chartsDir); err != nil {
		log.Printf("skipping throughput-vs-concurrency chart: %v", err)
	} else {
		log.Printf("wrote %s", path)
	}

	if path, err := report.LatencyPercentiles(rep, *latencyPhase, *chartsDir); err != nil {
		log.Printf("skipping latency-percentiles chart: %v", err)
	} else {
		log.Printf("wrote %s", path)
	}

	if path, err := report.ThroughputVsReplicas(rep, *chartsDir); err != nil {
		log.Printf("skipping throughput-vs-replicas chart: %v", err)
	} else {
		log.Printf("wrote %s", path)
	}
}
