package sink

import "context"

type Event struct {
	DeviceID           string
	IdempotencyKey     string
	EventTimeUnixNanos int64
	Metric             string
	Value              float64
}

type Sink interface {
	Write(ctx context.Context, e Event) error
}

type Flusher interface {
	Flush(ctx context.Context) error
}

type PoisonedEvent struct {
	Event Event
	Err   error
}

type PoisonReporter interface {
	DrainPoisoned() []PoisonedEvent
}
