package window

import (
	"context"
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// base and evAt are defined in tumbling_test.go (same package).

func TestSliding_EventMapsToMultipleWindows(t *testing.T) {
	// size=20s, slide=10s → each event touches 2 windows.
	m := NewSlidingManager(20*time.Second, 10*time.Second, nil, nil, nil, nil)
	// base = 10:00:00, aligned to 10s → belongs to [base-10s, base+10s) and [base, base+20s).
	m.add(evAt("api", base.Unix()))
	if got := len(m.windows); got != 2 {
		t.Fatalf("window count = %d, want 2 (event touches 2 overlapping windows)", got)
	}
}

func TestSliding_OverlapWindowContainsBothEvents(t *testing.T) {
	// size=20s, slide=10s.
	// Event A at base (10:00:00): windows [base-10s, base+10s) and [base, base+20s).
	// Event B at base+15s: windows [base+10s, base+30s) and [base, base+20s).
	// Both share [base, base+20s).
	m := NewSlidingManager(20*time.Second, 10*time.Second, nil, nil, nil, nil)
	m.add(evAt("api", base.Unix()))
	m.add(evAt("api", base.Add(15*time.Second).Unix()))

	sharedKey := Key{GroupKey: "api", WindowStart: base}
	b := m.windows[sharedKey]
	if b == nil {
		t.Fatal("expected shared window [base, base+20s) to exist")
	}
	if b.Count != 2 {
		t.Fatalf("shared window count = %d, want 2", b.Count)
	}
}

func TestSliding_CloseExpired_ClosesWindowsAtWatermark(t *testing.T) {
	out := make(chan Batch, 8)
	m := NewSlidingManager(20*time.Second, 10*time.Second, nil, out, nil, nil)

	// Event A at base+5s → opens [base-10s, base+10s) and [base, base+20s).
	m.add(evAt("api", base.Add(5*time.Second).Unix()))

	// Advance watermark to base+11s via another api event; also opens
	// [base+10s, base+30s) and re-touches [base, base+20s).
	m.add(evAt("api", base.Add(11*time.Second).Unix()))
	// wm = base+11s → [base-10s, base+10s).end = base+10s ≤ wm → closes.
	m.closeExpired(context.Background(), base.Add(11*time.Second))

	select {
	case batch := <-out:
		wantEnd := base.Add(10 * time.Second)
		if !batch.WindowEnd.Equal(wantEnd) {
			t.Fatalf("window end = %s, want %s", batch.WindowEnd, wantEnd)
		}
	default:
		t.Fatal("expected one closed batch but got none")
	}

	// [base-10s, base+10s) must no longer be in the open set.
	closedStart := base.Add(-10 * time.Second)
	if _, exists := m.windows[Key{GroupKey: "api", WindowStart: closedStart}]; exists {
		t.Fatal("[base-10s, base+10s) should have been closed")
	}
}

func TestSliding_CloseExpired_ProcessingTimeFallback(t *testing.T) {
	out := make(chan Batch, 8)
	m := NewSlidingManager(20*time.Second, 10*time.Second, nil, out, nil, nil)

	// Event at base → [base-10s, base+10s) and [base, base+20s).
	m.add(evAt("api", base.Unix()))

	// No newer event (watermark stays at base), but wall clock is far in the future.
	m.closeExpired(context.Background(), base.Add(time.Hour))

	var batches []Batch
	for {
		select {
		case b := <-out:
			batches = append(batches, b)
			continue
		default:
		}
		break
	}
	if len(batches) != 2 {
		t.Fatalf("got %d batches, want 2 (both windows flushed by processing time)", len(batches))
	}
}

func TestSliding_ClosedWindowNotReopened(t *testing.T) {
	out := make(chan Batch, 8)
	m := NewSlidingManager(20*time.Second, 10*time.Second, nil, out, nil, nil)

	// Event at base → touches [base-10s, base+10s) and [base, base+20s).
	m.add(evAt("api", base.Unix()))

	// Flush both windows via processing-time fallback.
	m.closeExpired(context.Background(), base.Add(time.Hour))
	for len(out) > 0 {
		<-out
	}

	// A late event with the same timestamp must not reopen the flushed windows.
	m.add(evAt("api", base.Unix()))
	m.closeExpired(context.Background(), base.Add(time.Hour))

	if len(out) != 0 {
		t.Fatalf("expected no new batches after closed window; got %d", len(out))
	}
}

func TestSliding_Run_FlushesViaTicker(t *testing.T) {
	in := make(chan model.Envelope, 4)
	out := make(chan Batch, 4)
	now := func() time.Time { return base.Add(time.Hour) }
	m := NewSlidingManager(40*time.Millisecond, 20*time.Millisecond, in, out, nil, now)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = m.Run(ctx) }()

	in <- model.Envelope{Event: evAt("api", base.Unix())}

	select {
	case batch := <-out:
		if batch.Key.GroupKey != "api" {
			t.Fatalf("unexpected batch: %+v", batch)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sliding window flush")
	}
}

func TestSliding_ThreeWindowsPerEvent(t *testing.T) {
	// size=30s, slide=10s → each event touches 3 windows.
	m := NewSlidingManager(30*time.Second, 10*time.Second, nil, nil, nil, nil)
	m.add(evAt("api", base.Unix()))
	if got := len(m.windows); got != 3 {
		t.Fatalf("window count = %d, want 3", got)
	}
}
