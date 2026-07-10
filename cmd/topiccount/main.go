package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated kafka broker addresses")
	topic := flag.String("topic", "telemetry.events.dlq", "kafka topic to count records in")
	group := flag.String("group", "topiccount-checker", "throwaway consumer group, should be unique per run")
	idleTimeout := flag.Duration("idle-timeout", 5*time.Second, "stop counting after this long with no new records")
	flag.Parse()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	defer client.Close()

	count := 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), *idleTimeout)
		fetches := client.PollFetches(ctx)
		cancel()
		n := fetches.NumRecords()
		if n == 0 {
			break
		}
		count += n
	}

	log.Printf("count=%d", count)
}
