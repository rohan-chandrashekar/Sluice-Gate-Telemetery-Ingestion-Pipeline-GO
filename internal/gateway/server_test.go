package gateway

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	sluicev1 "github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/gen/sluice/v1"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/pool"
	"github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/internal/sink"
)

func startTestServer(t *testing.T, s sink.Sink, workers, queue int, shed bool) (sluicev1.IngestClient, *pool.Pool, func()) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	p := pool.New(workers, queue, s, nil)
	grpcServer := grpc.NewServer()
	sluicev1.RegisterIngestServer(grpcServer, New(p, shed, nil))

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
		p.Close()
	}

	return sluicev1.NewIngestClient(conn), p, cleanup
}

func TestPushAcceptsEvents(t *testing.T) {
	memSink := sink.NewMemorySink()
	client, _, cleanup := startTestServer(t, memSink, 4, 100, false)
	defer cleanup()

	req := &sluicev1.IngestRequest{
		Events: []*sluicev1.TelemetryEvent{
			{DeviceId: "d1", IdempotencyKey: "k1", Metric: "cpu", Value: 1},
			{DeviceId: "d2", IdempotencyKey: "k2", Metric: "cpu", Value: 2},
		},
	}

	resp, err := client.Push(context.Background(), req)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if resp.GetAccepted() != 2 || resp.GetRejected() != 0 {
		t.Fatalf("expected accepted=2 rejected=0, got accepted=%d rejected=%d", resp.GetAccepted(), resp.GetRejected())
	}
}

func TestPushRejectsWhenContextAlreadyCancelled(t *testing.T) {
	memSink := sink.NewMemorySink()
	client, _, cleanup := startTestServer(t, memSink, 0, 0, false)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := &sluicev1.IngestRequest{
		Events: []*sluicev1.TelemetryEvent{
			{DeviceId: "d1", IdempotencyKey: "k1"},
		},
	}

	_, err := client.Push(ctx, req)
	if err == nil {
		t.Fatal("expected error for already-cancelled context")
	}
}

type blockingShedSink struct {
	release chan struct{}
}

func (s *blockingShedSink) Write(_ context.Context, _ sink.Event) error {
	<-s.release
	return nil
}

func TestPushShedsWhenQueueFullInShedMode(t *testing.T) {
	blocked := &blockingShedSink{release: make(chan struct{})}

	client, _, cleanup := startTestServer(t, blocked, 1, 1, true)
	defer func() {
		close(blocked.release)
		cleanup()
	}()

	req := &sluicev1.IngestRequest{
		Events: []*sluicev1.TelemetryEvent{
			{DeviceId: "d1", IdempotencyKey: "k1"},
			{DeviceId: "d2", IdempotencyKey: "k2"},
			{DeviceId: "d3", IdempotencyKey: "k3"},
			{DeviceId: "d4", IdempotencyKey: "k4"},
		},
	}

	resp, err := client.Push(context.Background(), req)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if resp.GetRejected() == 0 {
		t.Fatalf("expected shed mode to reject at least one event once the queue filled, got rejected=%d accepted=%d",
			resp.GetRejected(), resp.GetAccepted())
	}
}
