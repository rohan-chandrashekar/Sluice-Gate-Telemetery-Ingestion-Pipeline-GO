package sink

import (
	"context"
	"sync"
	"testing"
)

func TestMemorySinkCountsConcurrentWrites(t *testing.T) {
	s := NewMemorySink()
	var wg sync.WaitGroup
	const n = 1000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_ = s.Write(context.Background(), Event{DeviceID: "d"})
		}()
	}
	wg.Wait()

	if got := s.Accepted(); got != n {
		t.Fatalf("expected %d accepted, got %d", n, got)
	}
}
