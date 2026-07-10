package pool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/sink"
)

type blockingSink struct {
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	written []sink.Event
}

func (s *blockingSink) Write(ctx context.Context, e sink.Event) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	s.mu.Lock()
	s.written = append(s.written, e)
	s.mu.Unlock()
	return nil
}

func (s *blockingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.written)
}

func TestSubmitBlocksWhenQueueFull(t *testing.T) {
	s := &blockingSink{entered: make(chan struct{}, 1), release: make(chan struct{})}
	p := New(1, 1, s, nil)
	defer func() {
		close(s.release)
		p.Close()
	}()

	longCtx, cancelLong := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelLong()

	if err := p.Submit(longCtx, sink.Event{DeviceID: "d1"}); err != nil {
		t.Fatalf("first submit should not block: %v", err)
	}

	select {
	case <-s.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never picked up first event")
	}

	if err := p.Submit(longCtx, sink.Event{DeviceID: "d2"}); err != nil {
		t.Fatalf("second submit fills queue capacity, should not error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := p.Submit(ctx, sink.Event{DeviceID: "d3"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded when queue+in-flight full, got %v", err)
	}
}

func TestPoolDrainsOnClose(t *testing.T) {
	s := &blockingSink{release: make(chan struct{})}
	close(s.release)
	p := New(4, 100, s, nil)

	for i := 0; i < 50; i++ {
		if err := p.Submit(context.Background(), sink.Event{DeviceID: "d"}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	p.Close()

	if got := s.count(); got != 50 {
		t.Fatalf("expected all 50 events drained, got %d", got)
	}
}

func TestQueueDepthReflectsBacklog(t *testing.T) {
	s := &blockingSink{release: make(chan struct{})}
	p := New(1, 10, s, nil)
	defer func() {
		close(s.release)
		p.Close()
	}()

	for i := 0; i < 5; i++ {
		if err := p.Submit(context.Background(), sink.Event{}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}

	time.Sleep(20 * time.Millisecond)
	if depth := p.QueueDepth(); depth < 3 {
		t.Fatalf("expected backlog to remain queued while sink blocked, depth=%d", depth)
	}
	if cap := p.QueueCap(); cap != 10 {
		t.Fatalf("expected cap 10, got %d", cap)
	}
}
