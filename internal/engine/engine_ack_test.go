package engine

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
	"github.com/flipslidersand/stream-rail/internal/rule"
)

func TestAckBarrier(t *testing.T) {
	var acks int32
	done := ackBarrier(func() { atomic.AddInt32(&acks, 1) }, 3)
	done()
	done()
	if got := atomic.LoadInt32(&acks); got != 0 {
		t.Fatalf("acked after %d/3 calls, want 0", got)
	}
	done()
	if got := atomic.LoadInt32(&acks); got != 1 {
		t.Fatalf("acks after 3 calls = %d, want 1", got)
	}

	// A nil ack yields a no-op barrier that must not panic.
	noop := ackBarrier(nil, 2)
	noop()
	noop()
}

// TestEngine_AcksOnceAcrossManagers verifies the end-to-end ack fires exactly
// once even though the event fans out to several window managers (#19).
func TestEngine_AcksOnceAcrossManagers(t *testing.T) {
	in := make(chan model.Envelope, 1)
	// Two distinct window sizes → two managers → the event is added twice.
	rules := []rule.Rule{
		{Name: "r-fast", GroupBy: "service", AggFunc: rule.AggCount, Having: rule.Having{Op: rule.OpGT, Value: 0}, WindowSize: 40 * time.Millisecond},
		{Name: "r-slow", GroupBy: "service", AggFunc: rule.AggCount, Having: rule.Having{Op: rule.OpGT, Value: 0}, WindowSize: 90 * time.Millisecond},
	}
	eng := New(in, time.Minute, rules, io.Discard, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = eng.Run(ctx) }()

	var acks int32
	in <- model.Envelope{
		Event: model.Event{Service: "api", Level: "ERROR", Timestamp: time.Now().Unix()},
		Ack:   func() { atomic.AddInt32(&acks, 1) },
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&acks) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Give any erroneous extra acks a chance to land before asserting.
	time.Sleep(50 * time.Millisecond)

	if got := atomic.LoadInt32(&acks); got != 1 {
		t.Fatalf("ack fired %d times across 2 managers, want exactly 1", got)
	}
}
