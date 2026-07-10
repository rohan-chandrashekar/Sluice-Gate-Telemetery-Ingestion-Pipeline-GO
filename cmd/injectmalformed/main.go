package main

import (
	"context"
	"flag"
	"log"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated kafka broker addresses")
	topic := flag.String("topic", "telemetry.events", "kafka topic to inject malformed records into")
	count := flag.Int("count", 10, "number of malformed records to inject")
	flag.Parse()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	defer client.Close()

	garbage := []byte{0xFF, 0xFF, 0xFF, 0x00, 0x01, 0x02, 0x9F}
	ctx := context.Background()

	for i := 0; i < *count; i++ {
		record := &kgo.Record{Topic: *topic, Key: []byte("malformed"), Value: garbage}
		if res := client.ProduceSync(ctx, record); res.FirstErr() != nil {
			log.Fatalf("produce malformed record %d: %v", i, res.FirstErr())
		}
	}

	log.Printf("injected %d malformed records into %s", *count, *topic)
}
