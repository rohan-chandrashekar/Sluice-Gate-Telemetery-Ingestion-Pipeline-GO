package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	sluicev1 "github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/gen/sluice/v1"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/gateway"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/metrics"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/pool"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/sink"
)

func main() {
	listen := flag.String("listen", ":50051", "grpc listen address")
	workers := flag.Int("workers", 32, "number of sink worker goroutines")
	queue := flag.Int("queue", 10000, "bounded queue size")
	sinkKind := flag.String("sink", "kafka", "sink implementation: memory|kafka")
	brokers := flag.String("brokers", "localhost:9092", "comma-separated kafka broker addresses")
	topic := flag.String("topic", "telemetry.events", "kafka topic to produce to")
	shed := flag.Bool("shed", false, "reject (never block) when the queue is full, instead of the default blocking backpressure")
	metricsAddr := flag.String("metrics-addr", ":9100", "address to serve /metrics on")
	flag.Parse()

	var s sink.Sink
	var closeSink func()

	switch *sinkKind {
	case "memory":
		s = sink.NewMemorySink()
		closeSink = func() {}
	case "kafka":
		ks, err := sink.NewKafkaSink(strings.Split(*brokers, ","), *topic)
		if err != nil {
			log.Fatalf("kafka sink: %v", err)
		}
		s = ks
		closeSink = ks.Close
	default:
		log.Fatalf("unknown sink kind %q", *sinkKind)
	}

	errCount := int64(0)
	p := pool.New(*workers, *queue, s, func(err error) {
		errCount++
		log.Printf("sink write error: %v", err)
	})

	reg := metrics.NewRegistry()
	gwMetrics := metrics.NewGateway(reg, func() float64 { return float64(p.QueueDepth()) })
	metricsSrv := metrics.Serve(*metricsAddr, reg)

	srv := grpc.NewServer()
	sluicev1.RegisterIngestServer(srv, gateway.New(p, *shed, gwMetrics))

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	go func() {
		log.Printf("gateway listening on %s sink=%s workers=%d queue=%d shed=%v metrics=%s",
			*listen, *sinkKind, *workers, *queue, *shed, *metricsAddr)
		if err := srv.Serve(lis); err != nil {
			log.Printf("serve: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	log.Println("shutting down, draining queue")
	srv.GracefulStop()
	p.Close()
	closeSink()
	metrics.Shutdown(metricsSrv)

	if ms, ok := s.(*sink.MemorySink); ok {
		log.Printf("drained, accepted=%d errors=%d", ms.Accepted(), errCount)
	} else {
		log.Printf("drained, errors=%d", errCount)
	}
}
