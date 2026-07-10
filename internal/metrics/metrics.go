package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Gateway struct {
	IngestTotal           prometheus.Counter
	QueueDepth            prometheus.GaugeFunc
	SubmitBlockSeconds    prometheus.Histogram
	RequestLatencySeconds prometheus.Histogram
	ShedTotal             prometheus.Counter
}

func NewGateway(reg *prometheus.Registry, queueDepthFunc func() float64) *Gateway {
	g := &Gateway{
		IngestTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sluice_gateway_ingest_events_total",
			Help: "Total telemetry events accepted by the gateway.",
		}),
		QueueDepth: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "sluice_gateway_queue_depth",
			Help: "Current depth of the gateway's bounded worker-pool queue.",
		}, queueDepthFunc),
		SubmitBlockSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "sluice_gateway_submit_block_seconds",
			Help:    "Time spent blocked submitting an event into the bounded queue.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 20),
		}),
		RequestLatencySeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "sluice_gateway_request_latency_seconds",
			Help:    "Push RPC handler latency.",
			Buckets: prometheus.DefBuckets,
		}),
		ShedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sluice_gateway_shed_total",
			Help: "Total events rejected because -shed mode is enabled and the queue was full.",
		}),
	}
	reg.MustRegister(g.IngestTotal, g.QueueDepth, g.SubmitBlockSeconds, g.RequestLatencySeconds, g.ShedTotal)
	return g
}

type Consumer struct {
	ConsumedTotal     prometheus.Counter
	DedupDroppedTotal prometheus.Counter
	DLQTotal          prometheus.Counter
	BatchFlushSeconds prometheus.Histogram
	GroupLag          prometheus.Gauge
}

func NewConsumer(reg *prometheus.Registry) *Consumer {
	c := &Consumer{
		ConsumedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sluice_consumer_consumed_total",
			Help: "Total events persisted by the consumer.",
		}),
		DedupDroppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sluice_consumer_dedup_dropped_total",
			Help: "Total events dropped because they were already seen (dedup).",
		}),
		DLQTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sluice_consumer_dlq_total",
			Help: "Total events sent to the dead-letter topic.",
		}),
		BatchFlushSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "sluice_consumer_batch_flush_seconds",
			Help:    "Sink batch flush latency.",
			Buckets: prometheus.DefBuckets,
		}),
		GroupLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sluice_consumer_group_lag",
			Help: "Total consumer group lag across all partitions.",
		}),
	}
	reg.MustRegister(c.ConsumedTotal, c.DedupDroppedTotal, c.DLQTotal, c.BatchFlushSeconds, c.GroupLag)
	return c
}

func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	reg.MustRegister(prometheus.NewGoCollector())
	return reg
}

func Serve(addr string, reg *prometheus.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		_ = srv.ListenAndServe()
	}()
	return srv
}

func Shutdown(srv *http.Server) {
	_ = srv.Close()
}
