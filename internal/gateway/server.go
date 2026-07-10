package gateway

import (
	"context"
	"time"

	sluicev1 "github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/gen/sluice/v1"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/metrics"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/pool"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/sink"
)

type Server struct {
	sluicev1.UnimplementedIngestServer
	Pool    *pool.Pool
	Shed    bool
	Metrics *metrics.Gateway
}

func New(p *pool.Pool, shed bool, m *metrics.Gateway) *Server {
	return &Server{Pool: p, Shed: shed, Metrics: m}
}

func (s *Server) Push(ctx context.Context, req *sluicev1.IngestRequest) (*sluicev1.IngestResponse, error) {
	start := time.Now()
	resp := &sluicev1.IngestResponse{}

	for _, ev := range req.GetEvents() {
		e := sink.Event{
			DeviceID:           ev.GetDeviceId(),
			IdempotencyKey:     ev.GetIdempotencyKey(),
			EventTimeUnixNanos: ev.GetEventTimeUnixNanos(),
			Metric:             ev.GetMetric(),
			Value:              ev.GetValue(),
		}

		if s.Shed {
			if !s.Pool.TrySubmit(e) {
				resp.Rejected++
				if s.Metrics != nil {
					s.Metrics.ShedTotal.Inc()
				}
				continue
			}
			resp.Accepted++
			if s.Metrics != nil {
				s.Metrics.IngestTotal.Inc()
			}
			continue
		}

		submitStart := time.Now()
		err := s.Pool.Submit(ctx, e)
		if s.Metrics != nil {
			s.Metrics.SubmitBlockSeconds.Observe(time.Since(submitStart).Seconds())
		}
		if err != nil {
			resp.Rejected++
			continue
		}
		resp.Accepted++
		if s.Metrics != nil {
			s.Metrics.IngestTotal.Inc()
		}
	}

	if s.Metrics != nil {
		s.Metrics.RequestLatencySeconds.Observe(time.Since(start).Seconds())
	}
	return resp, nil
}
