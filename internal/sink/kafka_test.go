package sink

import (
	"testing"

	"google.golang.org/protobuf/proto"

	sluicev1 "github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/gen/sluice/v1"
)

func TestEventMarshalsToExpectedProtoWireFormat(t *testing.T) {
	e := Event{
		DeviceID:           "device-1",
		IdempotencyKey:     "k1",
		EventTimeUnixNanos: 42,
		Metric:             "cpu.util",
		Value:              3.14,
	}

	raw, err := proto.Marshal(&sluicev1.TelemetryEvent{
		DeviceId:           e.DeviceID,
		IdempotencyKey:     e.IdempotencyKey,
		EventTimeUnixNanos: e.EventTimeUnixNanos,
		Metric:             e.Metric,
		Value:              e.Value,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded sluicev1.TelemetryEvent
	if err := proto.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.GetDeviceId() != e.DeviceID || decoded.GetValue() != e.Value {
		t.Fatalf("round trip mismatch: %+v", &decoded)
	}
}
