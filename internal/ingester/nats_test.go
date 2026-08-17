package ingester

import (
	"testing"

	"github.com/flipslidersand/stream-rail/internal/model"
)

func TestDecodeEvent_Valid(t *testing.T) {
	ev, err := decodeEvent([]byte(`{"service":"api","level":"ERROR","ts":1718000000}`))
	if err != nil {
		t.Fatalf("decodeEvent: %v", err)
	}
	if ev.Service != "api" || ev.Level != "ERROR" || ev.Timestamp != 1718000000 {
		t.Fatalf("decoded = %+v", ev)
	}
}

func TestDecodeEvent_Invalid(t *testing.T) {
	if _, err := decodeEvent([]byte(`{not json`)); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestNewNATS_DurableName(t *testing.T) {
	ch := make(chan model.Event, 1)
	n := NewNATS("nats://localhost:4222", "application_logs", ch)
	if n.durable != "streamrail-application_logs" {
		t.Fatalf("durable = %q, want streamrail-application_logs", n.durable)
	}
	if n.subject != "application_logs" {
		t.Fatalf("subject = %q", n.subject)
	}
}
