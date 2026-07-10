package sink

import (
	"context"
	"sync/atomic"
)

type MemorySink struct {
	accepted atomic.Uint64
}

func NewMemorySink() *MemorySink {
	return &MemorySink{}
}

func (s *MemorySink) Write(_ context.Context, _ Event) error {
	s.accepted.Add(1)
	return nil
}

func (s *MemorySink) Accepted() uint64 {
	return s.accepted.Load()
}
