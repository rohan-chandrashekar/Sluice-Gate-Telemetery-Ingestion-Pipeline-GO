package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/consumer"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/dedup"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/dlq"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/metrics"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/sink"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated kafka broker addresses")
	topic := flag.String("topic", "telemetry.events", "kafka topic to consume")
	group := flag.String("group", "sluice-sink", "kafka consumer group id")
	sinkKind := flag.String("sink", "memory", "sink implementation: memory|timescale")
	maxRetries := flag.Int("max-retries", 3, "max persist retries before sending a record to the DLQ")

	timescaleDSN := flag.String("timescale-dsn", "postgres://sluice:sluice@localhost:5432/sluice", "timescale/postgres connection string")
	batchSize := flag.Int("batch", 500, "timescale sink batch size before flush")
	flushInterval := flag.Duration("flush-interval", 2*time.Second, "timescale sink max time between flushes")

	dedupEnabled := flag.Bool("dedup", false, "enable redis-backed dedup before persist")
	redisAddr := flag.String("redis-addr", "localhost:6379", "redis address for dedup")
	dedupTTL := flag.Duration("dedup-ttl", 10*time.Minute, "redis dedup key ttl")

	dlqEnabled := flag.Bool("dlq", false, "enable dead-letter production for poison/unrecoverable records")
	dlqTopic := flag.String("dlq-topic", "telemetry.events.dlq", "kafka topic for dead-lettered records")

	metricsAddr := flag.String("metrics-addr", ":9101", "address to serve /metrics on")

	flag.Parse()

	brokerList := strings.Split(*brokers, ",")

	var s sink.Sink
	switch *sinkKind {
	case "memory":
		s = sink.NewMemorySink()
	case "timescale":
		ts, err := sink.NewTimescaleSink(context.Background(), *timescaleDSN, *batchSize, *flushInterval, *maxRetries)
		if err != nil {
			log.Fatalf("timescale sink: %v", err)
		}
		defer ts.Close()
		s = ts
	default:
		log.Fatalf("unknown sink kind %q", *sinkKind)
	}

	var deduper *dedup.Deduper
	if *dedupEnabled {
		deduper = dedup.New(*redisAddr, *dedupTTL)
		defer deduper.Close()
	}

	var dlqProducer *dlq.Producer
	if *dlqEnabled {
		var err error
		dlqProducer, err = dlq.New(brokerList, *dlqTopic)
		if err != nil {
			log.Fatalf("dlq producer: %v", err)
		}
		defer dlqProducer.Close()
	}

	reg := metrics.NewRegistry()
	consMetrics := metrics.NewConsumer(reg)
	metricsSrv := metrics.Serve(*metricsAddr, reg)
	defer metrics.Shutdown(metricsSrv)

	c, err := consumer.New(consumer.Config{
		Brokers:    brokerList,
		Topic:      *topic,
		Group:      *group,
		MaxRetries: *maxRetries,
	}, s, deduper, dlqProducer, consMetrics)
	if err != nil {
		log.Fatalf("consumer.New: %v", err)
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("consumer starting brokers=%s topic=%s group=%s sink=%s dedup=%v dlq=%v metrics=%s",
		*brokers, *topic, *group, *sinkKind, *dedupEnabled, *dlqEnabled, *metricsAddr)
	if err := c.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("consumer run ended: %v", err)
	}

	if ms, ok := s.(*sink.MemorySink); ok {
		log.Printf("final memory sink accepted=%d consumed=%d deduped=%d dlq=%d", ms.Accepted(), c.Consumed(), c.Deduped(), c.DLQSent())
	} else {
		log.Printf("final consumed=%d deduped=%d dlq=%d", c.Consumed(), c.Deduped(), c.DLQSent())
	}
}
