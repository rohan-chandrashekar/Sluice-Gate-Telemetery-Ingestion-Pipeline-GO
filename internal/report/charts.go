package report

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

func distinctPhases(sweeps []SweepPoint) []string {
	seen := map[string]bool{}
	var phases []string
	for _, s := range sweeps {
		if !seen[s.Phase] {
			seen[s.Phase] = true
			phases = append(phases, s.Phase)
		}
	}
	sort.Strings(phases)
	return phases
}

func ThroughputVsConcurrency(rep *Report, outDir string) (string, error) {
	if len(rep.Sweeps) == 0 {
		return "", fmt.Errorf("no sweep data to chart")
	}

	p := plot.New()
	p.Title.Text = "Throughput vs Concurrency"
	p.X.Label.Text = "Concurrency"
	p.Y.Label.Text = "Events/sec (log scale)"
	p.Y.Scale = plot.LogScale{}
	p.Y.Tick.Marker = plot.LogTicks{}

	phases := distinctPhases(rep.Sweeps)
	for i, phase := range phases {
		var pts plotter.XYs
		for _, s := range rep.Sweeps {
			if s.Phase != phase {
				continue
			}
			pts = append(pts, plotter.XY{X: float64(s.Concurrency), Y: s.ThroughputEventsPerSec})
		}
		sort.Slice(pts, func(a, b int) bool { return pts[a].X < pts[b].X })

		line, err := plotter.NewLine(pts)
		if err != nil {
			return "", err
		}
		line.Color = paletteColor(i)
		p.Add(line)
		p.Legend.Add(phase, line)
	}
	p.Legend.Top = true

	return savePNG(p, outDir, "throughput-vs-concurrency.png")
}

func LatencyPercentiles(rep *Report, phase, outDir string) (string, error) {
	var pts []SweepPoint
	for _, s := range rep.Sweeps {
		if s.Phase == phase {
			pts = append(pts, s)
		}
	}
	if len(pts) == 0 {
		return "", fmt.Errorf("no sweep data for phase %s to chart", phase)
	}
	sort.Slice(pts, func(a, b int) bool { return pts[a].Concurrency < pts[b].Concurrency })

	p := plot.New()
	p.Title.Text = fmt.Sprintf("Latency Percentiles (%s)", phase)
	p.X.Label.Text = "Concurrency"
	p.Y.Label.Text = "Latency (ms)"

	series := []struct {
		name string
		get  func(SweepPoint) float64
	}{
		{"p50", func(s SweepPoint) float64 { return s.LatencyMsP50 }},
		{"p95", func(s SweepPoint) float64 { return s.LatencyMsP95 }},
		{"p99", func(s SweepPoint) float64 { return s.LatencyMsP99 }},
	}

	for i, ser := range series {
		var xys plotter.XYs
		for _, s := range pts {
			xys = append(xys, plotter.XY{X: float64(s.Concurrency), Y: ser.get(s)})
		}
		line, err := plotter.NewLine(xys)
		if err != nil {
			return "", err
		}
		line.Color = paletteColor(i)
		p.Add(line)
		p.Legend.Add(ser.name, line)
	}
	p.Legend.Top = true

	return savePNG(p, outDir, "latency-percentiles.png")
}

func ThroughputVsReplicas(rep *Report, outDir string) (string, error) {
	if rep.Scaling == nil || rep.Scaling.Blocked || len(rep.Scaling.Points) == 0 {
		return "", fmt.Errorf("no valid (non-blocked) phase4 scaling data to chart")
	}

	p := plot.New()
	p.Title.Text = "Throughput vs Consumer Replicas"
	p.X.Label.Text = "Consumer Replicas"
	p.Y.Label.Text = "Events/sec"

	var pts plotter.XYs
	for _, sp := range rep.Scaling.Points {
		pts = append(pts, plotter.XY{X: float64(sp.Replicas), Y: sp.ThroughputEventsPerSec})
	}
	line, err := plotter.NewLine(pts)
	if err != nil {
		return "", err
	}
	line.Color = paletteColor(0)
	p.Add(line)

	return savePNG(p, outDir, "throughput-vs-replicas.png")
}

func savePNG(p *plot.Plot, outDir, filename string) (string, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(outDir, filename)
	if err := p.Save(6*vg.Inch, 4*vg.Inch, path); err != nil {
		return "", err
	}
	return path, nil
}

func paletteColor(i int) *colorRGBA {
	palette := []colorRGBA{
		{31, 119, 180, 255},
		{255, 127, 14, 255},
		{44, 160, 44, 255},
		{214, 39, 40, 255},
		{148, 103, 189, 255},
	}
	c := palette[i%len(palette)]
	return &c
}

type colorRGBA struct {
	R, G, B, A uint8
}

func (c colorRGBA) RGBA() (r, g, b, a uint32) {
	toU32 := func(v uint8) uint32 {
		u := uint32(v)
		return u | u<<8
	}
	return toU32(c.R), toU32(c.G), toU32(c.B), toU32(c.A)
}

var _ = math.NaN
