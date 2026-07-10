package dlq

import (
	"context"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
	topic  string
}

func New(brokers []string, topic string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{client: client, topic: topic}, nil
}

func (p *Producer) Send(ctx context.Context, originalTopic string, partition int32, offset int64, key, value []byte, cause error) error {
	record := &kgo.Record{
		Topic: p.topic,
		Key:   key,
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "error", Value: []byte(cause.Error())},
			{Key: "original-topic", Value: []byte(originalTopic)},
			{Key: "original-partition", Value: []byte(strconv.Itoa(int(partition)))},
			{Key: "original-offset", Value: []byte(strconv.FormatInt(offset, 10))},
		},
	}
	res := p.client.ProduceSync(ctx, record)
	return res.FirstErr()
}

func (p *Producer) Close() {
	_ = p.client.Flush(context.Background())
	p.client.Close()
}
