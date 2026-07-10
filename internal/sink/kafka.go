package sink

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	sluicev1 "github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/gen/sluice/v1"
)

type KafkaSink struct {
	client *kgo.Client
	topic  string
}

func NewKafkaSink(brokers []string, topic string) (*KafkaSink, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(4<<20),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaSink{client: client, topic: topic}, nil
}

func (s *KafkaSink) Write(ctx context.Context, e Event) error {
	val, err := proto.Marshal(&sluicev1.TelemetryEvent{
		DeviceId:           e.DeviceID,
		IdempotencyKey:     e.IdempotencyKey,
		EventTimeUnixNanos: e.EventTimeUnixNanos,
		Metric:             e.Metric,
		Value:              e.Value,
	})
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic: s.topic,
		Key:   []byte(e.DeviceID),
		Value: val,
	}

	results := s.client.ProduceSync(ctx, record)
	return results.FirstErr()
}

func (s *KafkaSink) Flush(ctx context.Context) error {
	return s.client.Flush(ctx)
}

func (s *KafkaSink) Close() {
	_ = s.client.Flush(context.Background())
	s.client.Close()
}
