package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/sink"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated kafka broker addresses")
	topic := flag.String("topic", "telemetry.losstest", "kafka topic to produce to")
	count := flag.Int("count", 200000, "exact number of events to produce")
	concurrency := flag.Int("concurrency", 16, "concurrent producer goroutines")
	flag.Parse()

	ks, err := sink.NewKafkaSink(strings.Split(*brokers, ","), *topic)
	if err != nil {
		log.Fatalf("kafka sink: %v", err)
	}

	perGoroutine := *count / *concurrency
	remainder := *count % *concurrency

	var wg sync.WaitGroup
	var produced atomic.Int64
	start := time.Now()

	for g := 0; g < *concurrency; g++ {
		n := perGoroutine
		if g < remainder {
			n++
		}
		wg.Add(1)
		go func(id, n int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				e := sink.Event{
					DeviceID:       fmt.Sprintf("device-%d", id),
					IdempotencyKey: fmt.Sprintf("loss-%d-%d", id, i),
					Metric:         "cpu.util",
					Value:          float64(i % 100),
				}
				for {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					writeErr := ks.Write(ctx, e)
					cancel()
					if writeErr == nil {
						break
					}
					log.Printf("producer %d retry after error: %v", id, writeErr)
					time.Sleep(200 * time.Millisecond)
				}
				produced.Add(1)
			}
		}(g, n)
	}

	wg.Wait()
	ks.Close()
	log.Printf("produced=%d in %s", produced.Load(), time.Since(start))
}
