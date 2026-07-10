package sink

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TimescaleSink struct {
	pool          *pgxpool.Pool
	batchSize     int
	flushInterval time.Duration
	maxRetries    int

	mu       sync.Mutex
	buffer   []Event
	poisoned []PoisonedEvent

	stop chan struct{}
	done chan struct{}
}

func NewTimescaleSink(ctx context.Context, dsn string, batchSize int, flushInterval time.Duration, maxRetries int) (*TimescaleSink, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	s := &TimescaleSink{
		pool:          pool,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		maxRetries:    maxRetries,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	go s.tickerLoop()
	return s, nil
}

func (s *TimescaleSink) tickerLoop() {
	defer close(s.done)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = s.Flush(context.Background())
		case <-s.stop:
			return
		}
	}
}

func (s *TimescaleSink) Write(_ context.Context, e Event) error {
	s.mu.Lock()
	s.buffer = append(s.buffer, e)
	shouldFlush := len(s.buffer) >= s.batchSize
	s.mu.Unlock()

	if shouldFlush {
		return s.Flush(context.Background())
	}
	return nil
}

func (s *TimescaleSink) drainBuffer() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch := s.buffer
	s.buffer = nil
	return batch
}

func (s *TimescaleSink) markPoisoned(e Event, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poisoned = append(s.poisoned, PoisonedEvent{Event: e, Err: err})
}

func (s *TimescaleSink) DrainPoisoned() []PoisonedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.poisoned
	s.poisoned = nil
	return p
}

func (s *TimescaleSink) Flush(ctx context.Context) error {
	batch := s.drainBuffer()
	if len(batch) == 0 {
		return nil
	}

	var err error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		err = s.copyBatch(ctx, batch)
		if err == nil {
			return nil
		}
	}

	for _, e := range batch {
		var rowErr error
		for attempt := 0; attempt <= s.maxRetries; attempt++ {
			rowErr = s.insertOne(ctx, e)
			if rowErr == nil {
				break
			}
		}
		if rowErr != nil {
			s.markPoisoned(e, rowErr)
		}
	}
	return nil
}

func (s *TimescaleSink) copyBatch(ctx context.Context, batch []Event) error {
	rows := make([][]any, len(batch))
	for i, e := range batch {
		rows[i] = []any{
			e.DeviceID,
			time.Unix(0, e.EventTimeUnixNanos).UTC(),
			e.Metric,
			e.Value,
			e.IdempotencyKey,
		}
	}
	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"telemetry_events"},
		[]string{"device_id", "event_time", "metric", "value", "idempotency_key"},
		pgx.CopyFromRows(rows),
	)
	return err
}

func (s *TimescaleSink) insertOne(ctx context.Context, e Event) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO telemetry_events (device_id, event_time, metric, value, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT DO NOTHING`,
		e.DeviceID, time.Unix(0, e.EventTimeUnixNanos).UTC(), e.Metric, e.Value, e.IdempotencyKey,
	)
	return err
}

func (s *TimescaleSink) Close() {
	close(s.stop)
	<-s.done
	_ = s.Flush(context.Background())
	s.pool.Close()
}
