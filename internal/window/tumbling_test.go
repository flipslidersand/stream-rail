package window

import (
	"context"
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// evAt builds an event for the given service at a Unix-second timestamp.
func evAt(service string, ts int64) model.Event {
	return model.Event{Service: service, Level: "ERROR", Timestamp: ts}
}

// base is 2024-06-10T10:00:00Z, aligned to a 5-minute boundary.
var base = time.Date(2024, 6, 10, 10, 0, 0, 0, time.UTC)

func TestAdd_TruncatesToWindowBoundary(t *testing.T) {
	m := NewManager(5*time.Minute, nil, nil, nil, nil)

	// 10:00:00, 10:02:30, 10:04:59 all belong to the 10:00–10:05 window.
	m.add(evAt("api", base.Unix()))
	m.add(evAt("api", base.Add(150*time.Second).Unix()))
	m.add(evAt("api", base.Add(299*time.Second).Unix()))
	// 10:05:00 opens the next window.
	m.add(evAt("api", base.Add(5*time.Minute).Unix()))

	if got := len(m.windows); got != 2 {
		t.Fatalf("window count = %d, want 2", got)
	}
	key := Key{GroupKey: "api", WindowStart: base}
	b := m.windows[key]
	if b == nil {
		t.Fatalf("expected bucket for %v", key)
	}
	if b.Count != 3 {
		t.Fatalf("first window count = %d, want 3", b.Count)
	}
}

func TestAdd_SeparatesGroups(t *testing.T) {
	m := NewManager(5*time.Minute, nil, nil, nil, nil)
	m.add(evAt("api", base.Unix()))
	m.add(evAt("web", base.Unix()))
	m.add(evAt("web", base.Add(10*time.Second).Unix()))

	if got := len(m.windows); got != 2 {
		t.Fatalf("window count = %d, want 2 (one per service)", got)
	}
	if b := m.windows[Key{GroupKey: "web", WindowStart: base}]; b == nil || b.Count != 2 {
		t.Fatalf("web bucket = %+v, want count 2", b)
	}
}

func TestCloseExpired_EmitsOnlyClosedWindows(t *testing.T) {
	out := make(chan Batch, 4)
	m := NewManager(5*time.Minute, nil, out, nil, nil)

	m.add(evAt("api", base.Unix()))                     // window 10:00–10:05
	m.add(evAt("api", base.Add(6*time.Minute).Unix()))  // window 10:05–10:10

	// now = 10:05:00 → first window is closed (end inclusive), second still open.
	m.closeExpired(context.Background(), base.Add(5*time.Minute))

	select {
	case batch := <-out:
		if !batch.Key.WindowStart.Equal(base) {
			t.Fatalf("emitted window start = %s, want %s", batch.Key.WindowStart, base)
		}
		if batch.Count != 1 || !batch.WindowEnd.Equal(base.Add(5*time.Minute)) {
			t.Fatalf("unexpected batch: %+v", batch)
		}
	default:
		t.Fatal("expected one closed window batch")
	}

	// The open window must remain.
	if got := len(m.windows); got != 1 {
		t.Fatalf("remaining windows = %d, want 1", got)
	}
	select {
	case b := <-out:
		t.Fatalf("did not expect a second batch, got %+v", b)
	default:
	}
}

func TestRun_FlushesViaTicker(t *testing.T) {
	in := make(chan model.Event, 8)
	out := make(chan Batch, 8)
	// Drive the clock forward so ticker-triggered closeExpired sees the window
	// as expired regardless of real event timestamps.
	now := func() time.Time { return base.Add(time.Hour) }
	m := NewManager(20*time.Millisecond, in, out, nil, now)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = m.Run(ctx) }()

	in <- evAt("api", base.Unix())

	select {
	case batch := <-out:
		if batch.Key.GroupKey != "api" || batch.Count != 1 {
			t.Fatalf("unexpected batch: %+v", batch)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for window flush")
	}
}
