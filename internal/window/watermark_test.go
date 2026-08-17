package window

import (
	"context"
	"testing"
	"time"
)

// These tests exercise Phase 6 watermark semantics. base (2024-06-10T10:00:00Z,
// aligned to any small window) and evAt are defined in tumbling_test.go.

func TestWatermark_CloseAndLateCorrection(t *testing.T) {
	out := make(chan Batch, 8)
	m := NewManager(10*time.Second, nil, out, nil, nil)
	m.SetLateness(5 * time.Second)

	// Window A = [base, base+10s): three on-time events.
	m.add(evAt("api", base.Unix()))
	m.add(evAt("api", base.Unix()))
	m.add(evAt("api", base.Unix()))

	// Advance the watermark: an event at base+16s → watermark = base+11s, which
	// is >= A.end (base+10s) but < A.end+lateness (base+15s), so A closes yet
	// stays correctable.
	m.add(evAt("api", base.Add(16*time.Second).Unix()))

	m.closeExpired(context.Background(), base.Add(16*time.Second))

	b1 := <-out
	if b1.Corrected || b1.Count != 3 || !b1.Key.WindowStart.Equal(base) {
		t.Fatalf("first batch = %+v, want window A count=3 not corrected", b1)
	}
	if _, open := m.windows[b1.Key]; open {
		t.Fatal("closed window should leave the open set")
	}
	if m.closed[b1.Key] == nil {
		t.Fatal("closed window should be retained for correction")
	}

	// A late event lands in the already-closed window A → corrected re-emit.
	m.add(evAt("api", base.Add(2*time.Second).Unix()))

	b2 := <-out
	if !b2.Corrected || b2.Count != 4 || !b2.Key.WindowStart.Equal(base) {
		t.Fatalf("second batch = %+v, want window A count=4 corrected", b2)
	}
}

func TestWatermark_GCDropsWindowBeyondLateness(t *testing.T) {
	out := make(chan Batch, 8)
	m := NewManager(10*time.Second, nil, out, nil, nil)
	m.SetLateness(5 * time.Second)

	m.add(evAt("api", base.Unix())) // window A
	// Watermark jumps well past A.end+lateness.
	m.add(evAt("api", base.Add(40*time.Second).Unix()))

	m.closeExpired(context.Background(), base.Add(40*time.Second))

	<-out // A's close batch
	if len(m.closed) != 0 {
		t.Fatalf("window A should be garbage-collected, closed=%d", len(m.closed))
	}
}

func TestWatermark_NoNewerEventKeepsWindowOpen(t *testing.T) {
	out := make(chan Batch, 8)
	m := NewManager(10*time.Second, nil, out, nil, nil)
	m.SetLateness(5 * time.Second)

	m.add(evAt("api", base.Unix()))
	// Watermark = base - 5s, far below A.end; processing clock hasn't reached
	// end+lateness either → window stays open.
	m.closeExpired(context.Background(), base.Add(3*time.Second))

	select {
	case b := <-out:
		t.Fatalf("did not expect a batch yet, got %+v", b)
	default:
	}
	if len(m.windows) != 1 {
		t.Fatalf("expected window A still open, windows=%d", len(m.windows))
	}
}

// Ensure the processing-time fallback still flushes when the stream goes idle
// (no newer events to advance the event-time watermark).
func TestWatermark_ProcessingFallbackFlushesIdle(t *testing.T) {
	out := make(chan Batch, 8)
	m := NewManager(10*time.Second, nil, out, nil, nil)
	m.SetLateness(5 * time.Second)

	m.add(evAt("api", base.Unix()))
	// now is well past end+lateness, watermark hasn't moved → fallback closes it.
	m.closeExpired(context.Background(), base.Add(time.Hour))

	b := <-out
	if b.Count != 1 || !b.Key.WindowStart.Equal(base) {
		t.Fatalf("fallback batch = %+v, want window A count=1", b)
	}
}
