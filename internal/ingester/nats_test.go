package ingester

import (
	"sync"
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// fakeDeduper is an in-memory Deduper for unit tests.
type fakeDeduper struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func newFakeDeduper() *fakeDeduper { return &fakeDeduper{seen: make(map[string]struct{})} }

func (f *fakeDeduper) Seen(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.seen[id]
	return ok
}

func (f *fakeDeduper) Mark(id string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen[id] = struct{}{}
	return nil
}

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
	ch := make(chan model.Envelope, 1)
	n := NewNATS("nats://localhost:4222", "application_logs", ch)
	if n.durable != "streamrail-application_logs" {
		t.Fatalf("durable = %q, want streamrail-application_logs", n.durable)
	}
	if n.subject != "application_logs" {
		t.Fatalf("subject = %q", n.subject)
	}
}

func TestNATSIngester_WithDeduper(t *testing.T) {
	ch := make(chan model.Envelope, 1)
	d := newFakeDeduper()
	n := NewNATS("nats://localhost:4222", "logs", ch).WithDeduper(d)
	if n.deduper == nil {
		t.Fatal("deduper should be set after WithDeduper")
	}
}

func TestFakeDeduper_SeenMark(t *testing.T) {
	d := newFakeDeduper()
	if d.Seen("x") {
		t.Fatal("should not be seen before Mark")
	}
	if err := d.Mark("x", time.Minute); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if !d.Seen("x") {
		t.Fatal("should be seen after Mark")
	}
	if d.Seen("y") {
		t.Fatal("y should not be seen")
	}
}
