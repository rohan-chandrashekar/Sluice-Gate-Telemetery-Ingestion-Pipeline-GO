package pool

import (
	"context"
	"sync"

	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/sink"
)

type Pool struct {
	queue chan sink.Event
	sink  sink.Sink
	wg    sync.WaitGroup
	errFn func(error)
}

func New(workers, queueSize int, s sink.Sink, errFn func(error)) *Pool {
	if errFn == nil {
		errFn = func(error) {}
	}
	p := &Pool{
		queue: make(chan sink.Event, queueSize),
		sink:  s,
		errFn: errFn,
	}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for e := range p.queue {
		if err := p.sink.Write(context.Background(), e); err != nil {
			p.errFn(err)
		}
	}
}

func (p *Pool) Submit(ctx context.Context, e sink.Event) error {
	select {
	case p.queue <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) TrySubmit(e sink.Event) bool {
	select {
	case p.queue <- e:
		return true
	default:
		return false
	}
}

func (p *Pool) QueueDepth() int {
	return len(p.queue)
}

func (p *Pool) QueueCap() int {
	return cap(p.queue)
}

func (p *Pool) Close() {
	close(p.queue)
	p.wg.Wait()
}
