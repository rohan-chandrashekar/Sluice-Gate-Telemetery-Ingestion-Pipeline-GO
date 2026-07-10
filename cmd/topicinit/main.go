package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated kafka broker addresses")
	topics := flag.String("topics", "telemetry.events", "comma-separated topic names to create or delete")
	partitions := flag.Int("partitions", 12, "partition count for each topic")
	replication := flag.Int("replication", 1, "replication factor for each topic")
	del := flag.Bool("delete", false, "delete the given topics instead of creating them")
	flag.Parse()

	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...))
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topicList := strings.Split(*topics, ",")

	if *del {
		resp, err := admin.DeleteTopics(ctx, topicList...)
		if err != nil {
			log.Fatalf("delete topics: %v", err)
		}
		for _, t := range resp {
			if t.Err != nil {
				log.Printf("topic %s: %v (may not exist)", t.Topic, t.Err)
			} else {
				log.Printf("topic %s deleted", t.Topic)
			}
		}
		return
	}

	resp, err := admin.CreateTopics(ctx, int32(*partitions), int16(*replication), nil, topicList...)
	if err != nil {
		log.Fatalf("create topics: %v", err)
	}

	for _, t := range resp {
		if t.Err != nil {
			log.Printf("topic %s: %v (may already exist)", t.Topic, t.Err)
		} else {
			log.Printf("topic %s created partitions=%d replication=%d", t.Topic, *partitions, *replication)
		}
	}
}
