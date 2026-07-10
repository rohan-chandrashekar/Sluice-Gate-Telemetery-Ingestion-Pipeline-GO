package consumer

import (
	"testing"

	"google.golang.org/protobuf/proto"

	sluicev1 "github.com/rohan-chandrashekar/Sluice-Gate-Telemetery-Ingestion-Pipeline-GO/gen/sluice/v1"
)

func TestDecodeRecordRoundTrip(t *testing.T) {
	original := &sluicev1.TelemetryEvent{
		DeviceId:           "device-42",
		IdempotencyKey:     "key-1",
		EventTimeUnixNanos: 1234567890,
		Metric:             "cpu.util",
		Value:              99.5,
	}

	raw, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	e, err := DecodeRecord(raw)
	if err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}

	if e.DeviceID != original.DeviceId ||
		e.IdempotencyKey != original.IdempotencyKey ||
		e.EventTimeUnixNanos != original.EventTimeUnixNanos ||
		e.Metric != original.Metric ||
		e.Value != original.Value {
		t.Fatalf("decoded event mismatch: %+v vs proto %+v", e, original)
	}
}

func TestDecodeRecordRejectsGarbage(t *testing.T) {
	garbage := []byte{0xFF, 0xFF, 0xFF, 0x00, 0x01, 0x02, 0x9F}
	if _, err := DecodeRecord(garbage); err == nil {
		t.Fatal("expected error decoding malformed bytes")
	}
}
