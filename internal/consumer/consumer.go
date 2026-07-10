package consumer

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	sluicev1 "github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/gen/sluice/v1"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/dedup"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/dlq"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/metrics"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/sink"
)

type Config struct {
	Brokers    []string
	Topic      string
	Group      string
	MaxRetries int
}

func DecodeRecord(value []byte) (sink.Event, error) {
	var ev sluicev1.TelemetryEvent
	if err := proto.Unmarshal(value, &ev); err != nil {
		return sink.Event{}, err
	}
	return sink.Event{
		DeviceID:           ev.GetDeviceId(),
		IdempotencyKey:     ev.GetIdempotencyKey(),
		EventTimeUnixNanos: ev.GetEventTimeUnixNanos(),
		Metric:             ev.GetMetric(),
		Value:              ev.GetValue(),
	}, nil
}

type Consumer struct {
	client     *kgo.Client
	admin      *kadm.Client
	sink       sink.Sink
	dedup      *dedup.Deduper
	dlq        *dlq.Producer
	group      string
	topic      string
	maxRetries int
	metrics    *metrics.Consumer

	consumed atomic.Uint64
	deduped  atomic.Uint64
	dlqSent  atomic.Uint64
}

func New(cfg Config, s sink.Sink, d *dedup.Deduper, q *dlq.Producer, m *metrics.Consumer) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.Group),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		client:     client,
		admin:      kadm.NewClient(client),
		sink:       s,
		dedup:      d,
		dlq:        q,
		group:      cfg.Group,
		topic:      cfg.Topic,
		maxRetries: cfg.MaxRetries,
		metrics:    m,
	}, nil
}

func (c *Consumer) Consumed() uint64 { return c.consumed.Load() }
func (c *Consumer) Deduped() uint64  { return c.deduped.Load() }
func (c *Consumer) DLQSent() uint64  { return c.dlqSent.Load() }

func (c *Consumer) Close() {
	c.client.Close()
}

func (c *Consumer) Run(ctx context.Context) error {
	go c.reportLoop(ctx)

	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fetches.IsClientClosed() {
			return nil
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error topic=%s partition=%d err=%v", topic, partition, err)
		})

		c.handleBatch(ctx, fetches.Records())

		if f, ok := c.sink.(sink.Flusher); ok {
			flushStart := time.Now()
			err := f.Flush(ctx)
			if c.metrics != nil {
				c.metrics.BatchFlushSeconds.Observe(time.Since(flushStart).Seconds())
			}
			if err != nil {
				log.Printf("flush error, withholding commit this round: %v", err)
				continue
			}
		}

		if pr, ok := c.sink.(sink.PoisonReporter); ok {
			for _, p := range pr.DrainPoisoned() {
				c.sendToDLQ(ctx, c.topic, -1, -1, []byte(p.Event.DeviceID), marshalEvent(p.Event), p.Err)
			}
		}

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}

type decodedRecord struct {
	record *kgo.Record
	event  sink.Event
}

func (c *Consumer) handleBatch(ctx context.Context, records []*kgo.Record) {
	decoded := make([]decodedRecord, 0, len(records))
	for _, record := range records {
		e, err := DecodeRecord(record.Value)
		if err != nil {
			log.Printf("unmarshal error, sending to DLQ: %v", err)
			c.sendToDLQ(ctx, record.Topic, record.Partition, record.Offset, record.Key, record.Value, err)
			continue
		}
		decoded = append(decoded, decodedRecord{record: record, event: e})
	}

	if len(decoded) == 0 {
		return
	}

	seen := make([]bool, len(decoded))
	if c.dedup != nil {
		keys := make([]string, len(decoded))
		for i, d := range decoded {
			keys[i] = d.event.IdempotencyKey
		}
		s, err := c.dedup.SeenBatch(ctx, keys)
		if err != nil {
			log.Printf("dedup batch check error, treating batch as not-seen: %v", err)
		} else {
			seen = s
		}
	}

	for i, d := range decoded {
		if seen[i] {
			c.deduped.Add(1)
			if c.metrics != nil {
				c.metrics.DedupDroppedTotal.Inc()
			}
			continue
		}
		c.persist(ctx, d.record, d.event)
	}
}

func (c *Consumer) persist(ctx context.Context, record *kgo.Record, e sink.Event) {
	if _, ok := c.sink.(sink.PoisonReporter); ok {
		if err := c.sink.Write(ctx, e); err != nil {
			log.Printf("sink write error (buffered sink, will surface via poison drain if terminal): %v", err)
		}
		c.consumed.Add(1)
		if c.metrics != nil {
			c.metrics.ConsumedTotal.Inc()
		}
		return
	}

	var writeErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		writeErr = c.sink.Write(ctx, e)
		if writeErr == nil {
			break
		}
	}
	if writeErr != nil {
		log.Printf("persist failed after %d retries, sending to DLQ: %v", c.maxRetries, writeErr)
		c.sendToDLQ(ctx, record.Topic, record.Partition, record.Offset, record.Key, record.Value, writeErr)
		return
	}
	c.consumed.Add(1)
	if c.metrics != nil {
		c.metrics.ConsumedTotal.Inc()
	}
}

func marshalEvent(e sink.Event) []byte {
	raw, err := proto.Marshal(&sluicev1.TelemetryEvent{
		DeviceId:           e.DeviceID,
		IdempotencyKey:     e.IdempotencyKey,
		EventTimeUnixNanos: e.EventTimeUnixNanos,
		Metric:             e.Metric,
		Value:              e.Value,
	})
	if err != nil {
		return nil
	}
	return raw
}

func (c *Consumer) sendToDLQ(ctx context.Context, topic string, partition int32, offset int64, key, value []byte, cause error) {
	if c.dlq == nil {
		log.Printf("no DLQ configured, dropping poison record: %v", cause)
		return
	}
	if err := c.dlq.Send(ctx, topic, partition, offset, key, value, cause); err != nil {
		log.Printf("dlq send failed: %v", err)
		return
	}
	c.dlqSent.Add(1)
	if c.metrics != nil {
		c.metrics.DLQTotal.Inc()
	}
}

func (c *Consumer) reportLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var last uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := c.consumed.Load()
			rate := float64(now-last) / 5.0
			last = now

			lag := int64(0)
			lagCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			lags, err := c.admin.Lag(lagCtx, c.group)
			cancel()
			if err == nil {
				lags.Each(func(l kadm.DescribedGroupLag) {
					lag += l.Lag.Total()
				})
			}
			if c.metrics != nil {
				c.metrics.GroupLag.Set(float64(lag))
			}

			log.Printf("consumed_total=%d rate=%.1f/s lag=%d deduped=%d dlq=%d", now, rate, lag, c.deduped.Load(), c.dlqSent.Load())
		}
	}
}
